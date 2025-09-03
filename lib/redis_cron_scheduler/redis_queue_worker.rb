# lib/redis_cron_scheduler/redis_queue_worker.rb
module RedisCronScheduler
  class RedisQueueWorker
    # --- Configuráveis via ENV ---
    POLL_INTERVAL        = (ENV['REDIS_POLL_INTERVAL'] || 10).to_f            # segundos quando fila vazia
    RUNNING_TTL          = (ENV['REDIS_RUNNING_TTL'] || 15).to_i            # lease por job (segundos)
    LEASE_RENEW_INTERVAL = (ENV['REDIS_LEASE_RENEW_INTERVAL'] || 5).to_i    # frequência de renovação do lease
    CLEANUP_INTERVAL     = (ENV['REDIS_CLEANUP_INTERVAL'] || 300).to_i       # varredura de cleanup (segundos)
    MAX_JOBS_PER_CYCLE   = (ENV['REDIS_MAX_JOBS_PER_CYCLE'] || 100).to_i
    SHUTDOWN_TIMEOUT     = (ENV['REDIS_SHUTDOWN_TIMEOUT'] || 30).to_i        # tempo para aguardar jobs terminarem no shutdown (s)
    REDIS_NAMESPACE      = (ENV['REDIS_NAMESPACE'] || "queue:").freeze

    # filas + peso (peso influencia chance de ser escolhida)
    QUEUE_WEIGHTS = {
        "critical" => 3,
        "mailers"  => 2,
        "default"  => 1
    }.freeze

    RETRY_INTERVALS = [5, 15, 30, 60, 300].freeze

    # --- Lua scripts (mantêm atomicidade de movimentos) ---
    LUA_MOVE_SCHEDULED = <<~LUA
        local scheduled_key = KEYS[1]
        local queue_key = KEYS[2]
        local now = tonumber(ARGV[1])
        local jobs = redis.call('ZRANGEBYSCORE', scheduled_key, 0, now)
        for i, job in ipairs(jobs) do
        redis.call('RPUSH', queue_key, job)
        redis.call('ZREM', scheduled_key, job)
        end
        return #jobs
    LUA

    LUA_MOVE_RETRY = <<~LUA
        local retry_key = KEYS[1]
        local queue_key = KEYS[2]
        local now = tonumber(ARGV[1])
        local jobs = redis.call('ZRANGEBYSCORE', retry_key, 0, now)
        for i, job in ipairs(jobs) do
        redis.call('RPUSH', queue_key, job)
        redis.call('ZREM', retry_key, job)
        end
        return #jobs
    LUA

    # LMOVE atômico (fila -> running). Lease é gerenciado depois no Ruby.
    LUA_POP_JOB = <<~LUA
        local queue_key = KEYS[1]
        local running_key = KEYS[2]
        return redis.call('LMOVE', queue_key, running_key, 'RIGHT', 'LEFT')
    LUA

    # --- Inicialização ---
    def self.start
        new.run
    end

    def initialize
        @worker_id = "#{Socket.gethostname}:#{Process.pid}:#{Thread.current.object_id}"
        @running = Concurrent::AtomicBoolean.new(true)
        @shutdown_immediate = Concurrent::AtomicBoolean.new(false)

        # estrutura para acompanhar leases que este worker criou
        @leases_mutex = Mutex.new
        @active_leases = {} # lease_key => expiration_time (apenas para inspeção rápida)

        # preparar array ponderado para seleção de fila
        @weighted_queues = build_weighted_queue_array(QUEUE_WEIGHTS)

        # controle de limpeza periódico
        @last_cleanup = Time.at(0)
    end

    # --- Loop principal ---
    def run
        Rails.logger.info "[RedisQueueWorker] Iniciando worker #{@worker_id}..."
        setup_signal_handlers

        # start lease renewer thread
        @renewer_thread = Thread.new { lease_renewer_loop }
        @last_config_check = Time.now

        # carregar scripts em cada conexão não estritamente necessário, usamos EVAL direto
        while @running.true?
        begin
            # cleanup periódico de stuck jobs
            if Time.now - @last_cleanup >= CLEANUP_INTERVAL
            cleanup_stuck_jobs
            @last_cleanup = Time.now
            end

            # mover scheduled/retry prontos
            move_scheduled_to_queue
            move_retry_to_queue


            processed = process_up_to_limit(MAX_JOBS_PER_CYCLE)

            # polling inteligente: só dorme quando não processou nada
            if processed == 0 && !@shutdown_immediate.true?
            sleep POLL_INTERVAL
            end
        rescue => e
            Rails.logger.error "[RedisQueueWorker] Erro no loop principal: #{e.class} - #{e.message}"
            Rails.logger.error e.backtrace.join("\n")
            sleep 1
        end
        end

        # se shutdown normal (graceful) aguardar jobs em andamento
        if !@shutdown_immediate.true?
        wait_for_active_jobs_or_timeout(SHUTDOWN_TIMEOUT)
        end

        # finalizar renewer
        @renewer_thread&.kill
        Rails.logger.info "[RedisQueueWorker] Worker #{@worker_id} finalizado."
    end

    def shutdown(immediate: false)
        @shutdown_immediate.make_true if immediate
        @running.make_false
        Rails.logger.info "[RedisQueueWorker] Shutdown solicitado (immediate: #{immediate})"
    end

    private

    # --------------- Redis helpers (usa connection pool do Rails)
    def with_redis
        Rails.cache.redis.with do |conn|
        yield conn
        end
    end

    def queue_key(queue);   "#{REDIS_NAMESPACE}queue:#{queue}"; end
    def running_key(queue); "#{REDIS_NAMESPACE}running:#{queue}:#{@worker_id}"; end
    def lease_key(queue, job_id); "#{REDIS_NAMESPACE}running_lease:#{queue}:#{job_id}"; end
    def retry_key(queue);   "#{REDIS_NAMESPACE}retry:#{queue}"; end
    def dead_key(queue);    "#{REDIS_NAMESPACE}dead:#{queue}"; end
    def scheduled_key(queue); "#{REDIS_NAMESPACE}schedule:#{queue}"; end

    # --------------- Weighted queue array
    def build_weighted_queue_array(weights_hash)
        arr = []
        weights_hash.each do |q, w|
        next if w.to_i <= 0
        w.times { arr << q.to_s }
        end
        arr.freeze
    end

    # Escolhe uma fila aleatoriamente ponderada
    def pick_weighted_queue
        @weighted_queues.sample
    end

    # --------------- Move scheduled -> queue
    def move_scheduled_to_queue
        with_redis do |conn|
        now = Time.now.to_f
        QUEUE_WEIGHTS.keys.each do |queue|
            begin
            moved = conn.eval(LUA_MOVE_SCHEDULED, [scheduled_key(queue), queue_key(queue)], [now])
            Rails.logger.debug "[RedisQueueWorker] moved #{moved} scheduled -> #{queue}" if moved.to_i > 0
            rescue => e
            Rails.logger.error "[RedisQueueWorker] Erro move_scheduled_to_queue #{queue}: #{e.message}"
            end
        end
        end
    end

    # --------------- Move retry -> queue
    def move_retry_to_queue
        with_redis do |conn|
        now = Time.now.to_f
        QUEUE_WEIGHTS.keys.each do |queue|
            begin
            moved = conn.eval(LUA_MOVE_RETRY, [retry_key(queue), queue_key(queue)], [now])
            Rails.logger.debug "[RedisQueueWorker] moved #{moved} retry -> #{queue}" if moved.to_i > 0
            rescue => e
            Rails.logger.error "[RedisQueueWorker] Erro move_retry_to_queue #{queue}: #{e.message}"
            end
        end
        end
    end

    # --------------- Processamento: até N jobs por ciclo (evita starvation)
    def process_up_to_limit(limit)
        processed = 0
        limit.times do
        break unless @running.true?
        found = process_one_job_from_any_queue
        break if found == 0
        processed += found
        end
        processed
    end

    # Tenta pop atômico (LMOVE) de acordo com prioridade ponderada.
    # Retorna 1 se processou um job, 0 caso contrário.
    def process_one_job_from_any_queue
        # Try several times picking weighted queues; this balances priorities but still probes other queues.
        trials = @weighted_queues.size
        trials.times do
        queue = pick_weighted_queue
        with_redis do |conn|
            begin
            job_json = conn.eval(LUA_POP_JOB, [queue_key(queue), running_key(queue)], [])
            rescue => e
            Rails.logger.error "[RedisQueueWorker] Erro LMOVE em #{queue}: #{e.message}"
            next
            end

            next unless job_json

            # processou
            process_single_job(job_json, queue, conn)
            return 1
        end
        end

        # se nenhum dos picks retornou job, realizar uma verificação direta nas filas (fallback)
        QUEUE_WEIGHTS.keys.each do |queue|
        with_redis do |conn|
            job_json = begin
            conn.eval(LUA_POP_JOB, [queue_key(queue), running_key(queue)], [])
            rescue => e
            Rails.logger.error "[RedisQueueWorker] Erro LMOVE fallback em #{queue}: #{e.message}"
            nil
            end
            next unless job_json
            process_single_job(job_json, queue, conn)
            return 1
        end
        end

        0
    end

    # --- Processa job que já está no running (job_json já movido atômicamente)
    def process_single_job(job_json, queue, conn)
        job_data = parse_job_json(job_json)
        unless job_data
        # job corrompido: joga no dead para análise
        Rails.logger.error "[RedisQueueWorker] Job inválido detectado na fila running: #{job_json.inspect}"
        conn.zadd(dead_key(queue), Time.now.to_f, job_json)
        # remove da running list onde foi colocado
        conn.lrem(running_key(queue), 0, job_json)
        return
        end

        job_id = job_data[:job_id] || Digest::SHA1.hexdigest(job_json)
        lease_k = lease_key(queue, job_id)

        # criar lease e registrar localmente
        conn.setex(lease_k, RUNNING_TTL, job_json)
        register_lease_local(lease_k, Time.now + RUNNING_TTL)

        begin
        run_job(job_json, queue, conn)
        # sucesso: remove da running da lista (a LMOVE já colocou o job lá)
        conn.lrem(running_key(queue), 0, job_json)
        rescue => e
        Rails.logger.error "[RedisQueueWorker] Erro inesperado ao processar job #{job_id}: #{e.class} - #{e.message}"
        # run_job já faz retry/dead quando apropriado
        ensure
        # remover lease local e redis (caso ainda exista)
        unregister_lease_local(lease_k)
        conn.del(lease_k) rescue nil
        end
    end

    def run_job(job_json, queue, conn)
        job_data = parse_job_json(job_json)
        return unless job_data

        klass_name  = job_data[:job_class]
        args        = job_data[:arguments] || []
        retry_count = job_data[:retry_count] || 0
        job_id      = job_data[:job_id] || Digest::SHA1.hexdigest(job_json)

        klass = klass_name.to_s.safe_constantize
        unless klass
        Rails.logger.error "[RedisQueueWorker] Classe inválida para job #{job_id}: #{klass_name.inspect}"
        conn.zadd(dead_key(queue), Time.now.to_f, job_json)
        return
        end

        Rails.logger.info "[RedisQueueWorker] Executando #{klass_name} (#{job_id}) fila=#{queue} retry=#{retry_count}"
        
        begin
        klass.new.perform(*args)
        Rails.logger.info "[RedisQueueWorker] Job concluído #{job_id} (#{klass_name})"
        
        true
        rescue => e
        Rails.logger.error "[RedisQueueWorker] Falha no job #{job_id} (#{klass_name}): #{e.class} - #{e.message}"
        
        false
        end
    end

    # ⬇️ NOVO MÉTODO: Tratamento especial para falhas de jobs cron ⬇️
    def handle_cron_job_failure(job_data, job_json, queue, conn, error, retry_count)
        max_retries = RETRY_INTERVALS.size
        
        if retry_count < max_retries
        # ⬇️ Tentar novamente (retry normal) ⬇️
        delay = RETRY_INTERVALS[retry_count]
        retry_job = job_data.merge(
            retry_count: retry_count + 1,
            last_error: "#{error.class}: #{error.message}",
            failed_at: Time.now.iso8601
        ).to_json

        conn.zadd(retry_key(queue), Time.now.to_f + delay, retry_job)
        Rails.logger.info "[RedisQueueWorker] Job cron #{job_data[:cron_name]} reenfileirado para retry em #{delay}s"
        else
        # ⬇️ MÁXIMO DE RETRIES ATINGIDO - REAGENDAR MESMO ASSIM ⬇️
        Rails.logger.error "[RedisQueueWorker] Job cron #{job_data[:cron_name]} excedeu retries, mas será reagendado (cron)"
        
        # Registrar no dead para análise, mas SEMPRE reagendar
        conn.zadd(dead_key(queue), Time.now.to_f, job_json)
        
        # ⬇️ REAGENDAR PARA PRÓXIMA OCORRÊNCIA ⬇️
        reschedule_cron_job(job_data, queue, conn)
        end
    end

    # ⬇️ MÉTODO EXISTENTE PARA JOBS REGULARES (não cron) ⬇️
    def handle_regular_job_failure(job_data, job_json, queue, conn, error, retry_count)
        max_retries = RETRY_INTERVALS.size
        
        if retry_count < max_retries
        delay = RETRY_INTERVALS[retry_count]
        retry_job = job_data.merge(
            retry_count: retry_count + 1,
            last_error: "#{error.class}: #{error.message}",
            failed_at: Time.now.iso8601
        ).to_json

        conn.zadd(retry_key(queue), Time.now.to_f + delay, retry_job)
        Rails.logger.info "[RedisQueueWorker] Job #{job_data[:job_id]} reenfileirado para retry em #{delay}s"
        else
        # ⬇️ JOBS REGULARES VÃO PARA DEAD (comportamento normal) ⬇️
        conn.zadd(dead_key(queue), Time.now.to_f, job_json)
        Rails.logger.error "[RedisQueueWorker] Job #{job_data[:job_id]} movido para dead (excedeu retries)"
        end
    end

    # ⬇️ MÉTODO DE REAGENDAMENTO (já existente) ⬇️
    def reschedule_cron_job(job_data, queue, conn)
        cron_expression = job_data[:cron_expression]
        cron_uuid = job_data[:cron_uuid]
        
        next_time = CronParser.next_occurrence(cron_expression, Time.current + 1.minute)
        
        new_job_data = job_data.merge(
        job_id: "cron_#{cron_uuid}_#{next_time.to_i}",
        scheduled_at: next_time.iso8601,
        retry_count: 0,  # ⬅️ Resetar retry count para próxima execução
        last_error: nil,
        failed_at: nil
        )

        conn.zadd(
        scheduled_key(queue), 
        next_time.to_f, 
        new_job_data.to_json
        )
        
        Rails.logger.info "[RedisQueueWorker] Job cron #{job_data[:cron_name]} reagendado para #{next_time}"
    end

    # --- Lease renewer (único thread por worker) ---
    def lease_renewer_loop
        loop do
        break unless @running.true?
        begin
            lease_keys = nil
            @leases_mutex.synchronize { lease_keys = @active_leases.keys.dup }
            unless lease_keys.empty?
            with_redis do |conn|
                lease_keys.each do |lk|
                # renova apenas se ainda existir no redis (proteção adicional)
                if conn.exists?(lk)
                    conn.expire(lk, RUNNING_TTL)
                    # atualizar nossa expiração local (apenas estimativa)
                    @leases_mutex.synchronize { @active_leases[lk] = Time.now + RUNNING_TTL }
                else
                    # remove local se não existir
                    @leases_mutex.synchronize { @active_leases.delete(lk) }
                end
                end
            end
            end
        rescue => e
            Rails.logger.error "[RedisQueueWorker] Erro no lease_renewer: #{e.class} - #{e.message}"
        ensure
            sleep LEASE_RENEW_INTERVAL
        end
        end
    end

    # Registra lease_local thread-safe
    def register_lease_local(lease_key, expiry_time)
        @leases_mutex.synchronize { @active_leases[lease_key] = expiry_time }
    end

    def unregister_lease_local(lease_key)
        @leases_mutex.synchronize { @active_leases.delete(lease_key) }
    end

    # --- Cleanup: reencaminha jobs em running sem lease ---
    def cleanup_stuck_jobs
        Rails.logger.info "[RedisQueueWorker] Iniciando cleanup_stuck_jobs..."
        with_redis do |conn|
        QUEUE_WEIGHTS.keys.each do |queue|
            cursor = "0"
            loop do
            cursor, keys = conn.scan(cursor, match: "#{REDIS_NAMESPACE}running:#{queue}:*", count: 200)
            keys.each do |running_k|
                begin
                jobs = conn.lrange(running_k, 0, -1)
                next if jobs.nil? || jobs.empty?
                jobs.each do |job_json|
                    jd = parse_job_json(job_json) rescue nil
                    next unless jd
                    job_id = jd[:job_id] || Digest::SHA1.hexdigest(job_json)
                    lease_k = lease_key(queue, job_id)
                    unless conn.exists?(lease_k)
                    Rails.logger.warn "[RedisQueueWorker] Cleanup: re-enfileirando job stuck #{job_id} (from #{running_k})"
                    conn.rpush(queue_key(queue), job_json)
                    conn.lrem(running_k, 0, job_json)
                    end
                end
                # remove lista running vazia
                conn.del(running_k) if conn.llen(running_k).to_i == 0
                rescue => e
                Rails.logger.error "[RedisQueueWorker] Erro no cleanup para #{running_k}: #{e.message}"
                end
            end
            break if cursor == "0"
            end
        end
        end
    end

    # --- Utilitários e fallback ---
    def parse_job_json(json)
        data = JSON.parse(json, symbolize_names: true)
        
        # ⬇️ COMPATIBILIDADE COM CRON_SCHEDULER ⬇️
        # Se não tiver os campos cron, mas tiver job_class, é um job regular
        if data[:job_class] && !data[:cron_expression]
        data.merge(
            cron_expression: nil,
            cron_uuid: nil,
            cron_name: "regular_job"
        )
        else
        data
        end
    rescue JSON::ParserError => e
        Rails.logger.error "[RedisQueueWorker] JSON inválido: #{json.inspect}"
        nil
    end




    def wait_for_active_jobs_or_timeout(timeout_seconds)
        start = Time.now
        loop do
        break if @active_leases.empty?
        break if Time.now - start >= timeout_seconds
        Rails.logger.info "[RedisQueueWorker] aguardando #{@active_leases.size} jobs finalizarem antes do shutdown..."
        sleep 1
        end

        if @active_leases.any?
        Rails.logger.warn "[RedisQueueWorker] Timeout ao aguardar jobs; reencaminhando leases restantes..."
        # re-enfileira qualquer job que ainda esteja no running sem aguardar conclusão
        with_redis do |conn|
            @leases_mutex.synchronize do
            @active_leases.keys.each do |lk|
                # tentamos extrair queue e job_id do lease key (formato conhecido)
                if lk =~ /\Arunning_lease:(.+?):(.+)\z/
                q = $1
                jid = $2
                # job body guard (se existir, já está no lease value)
                job_json = conn.get(lk) rescue nil
                if job_json
                    conn.rpush(queue_key(q), job_json) rescue nil
                end
                conn.del(lk) rescue nil
                else
                # nome fora do padrão: apenas delete para evitar ostentação
                conn.del(lk) rescue nil
                end
                @active_leases.delete(lk)
            end
            end
        end
        end
    end

    def setup_signal_handlers
        # ⬇️ Use $stdout.puts em vez de Rails.logger ⬇️
        Signal.trap("TERM") do
        $stdout.puts "[RedisQueueWorker] Sinal TERM recebido, iniciando shutdown..."
        shutdown(immediate: true)
        end
        
        Signal.trap("INT") do
        $stdout.puts "[RedisQueueWorker] Sinal INT recebido, iniciando shutdown..."
        shutdown(immediate: true)
        end
    end

    


    # Métodos de classe para configuração
    class << self
      def start_workers
        worker_count = (ENV['REDIS_WORKER_COUNT'] || [3, Concurrent.processor_count].min).to_i
        
        worker_count.times do |i|
          Thread.new do
            begin
              new.run
            rescue => e
              Rails.logger.error "[RedisQueueWorker] Worker #{i} crashed: #{e.message}"
              sleep 5
              retry
            end
          end
        end
      end
    end
  end
end