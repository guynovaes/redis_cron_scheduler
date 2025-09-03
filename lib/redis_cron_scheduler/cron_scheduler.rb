# lib/redis_cron_scheduler/cron_scheduler.rb
module RedisCronScheduler
  class CronScheduler
    # ... (todo o código do CronScheduler aqui)
    # Apenas mude as constantes para configuração:
    CONFIG_FILE = File.expand_path("config/scheduled_jobs.json", Dir.pwd)

    REDIS_NAMESPACE      = (ENV['REDIS_NAMESPACE'] || "queue:").freeze
    LOCK_KEY = "#{REDIS_NAMESPACE}cron_scheduler:lock"
    POLL_INTERVAL = 45.seconds
    LOCK_TIMEOUT = 30.seconds 
    CONFIG_CHECK_INTERVAL = 300.seconds

    class << self
        def start
            new.run
        end

        def shutdown
            @running = false
            Rails.logger.info "[CronScheduler] Desligamento solicitado..."
        end

        def running?
            @running == true
        end
    end

    def initialize
        @running = Concurrent::AtomicBoolean.new(true)  # Use AtomicBoolean
        @scheduled_jobs = {}
        @last_config_check = Time.now

        Rails.logger.info "[CronScheduler] Timezone configurado: #{Time.zone.name}"
        Rails.logger.info "[CronScheduler] Hora atual: #{Time.current.strftime('%H:%M:%S %Z')}"

        setup_signal_handlers
        load_and_schedule_jobs
    end

    def run
        Rails.logger.info "[CronScheduler] Iniciando agendador..."
        iteration = 0
        
        while @running.true?
        iteration += 1
        Rails.logger.debug "[CronScheduler] Iteração ##{iteration} - #{Time.now.strftime('%H:%M:%S')}"
        
        begin
            # Usar lock distribuído para evitar execução duplicada
            with_redis_lock do
            check_config_changes
            execute_due_jobs
            end
            
            sleep_gracefully(POLL_INTERVAL)
        rescue => e
            Rails.logger.error "[CronScheduler] Erro no loop principal: #{e.message}"
            sleep_gracefully(5)
        end
        end
    end


    def shutdown
        @running.make_false  # ⬅️ Use make_false para AtomicBoolean
        Rails.logger.info "[CronScheduler] Desligando..."
    end

    private

    def sleep_gracefully(seconds)
        # ⬇️ Dormir o tempo total, mas verificar @running periodicamente ⬇️
        end_time = Time.now + seconds
        while Time.now < end_time && @running
        sleep [1, end_time - Time.now].min  # Verificar a cada 1 segundo
        end
    end

    def with_redis_lock(&block)
        with_redis do |conn|
        lock_acquired = conn.set(LOCK_KEY, Process.pid, nx: true, ex: LOCK_TIMEOUT)
        if lock_acquired
            begin
            Rails.logger.debug "[CronScheduler] 🔒 Lock adquirido"
            yield
            ensure
            conn.del(LOCK_KEY) if conn.get(LOCK_KEY) == Process.pid.to_s
            Rails.logger.debug "[CronScheduler] 🔓 Lock liberado"
            end
        else
            # ⬇️ NÃO BLOQUEIE - apenas log e continue ⬇️
            current_pid = conn.get(LOCK_KEY)
            Rails.logger.debug "[CronScheduler] ⏩ Lock ocupado (PID: #{current_pid}), pulando execução"
        end
        end
    end

    def check_config_changes
        return unless Time.now - @last_config_check >= CONFIG_CHECK_INTERVAL

        load_and_schedule_jobs
        @last_config_check = Time.now
    end

    def load_and_schedule_jobs
        config = load_config
        current_jobs = config[:jobs] || []
        
        Rails.logger.info "[CronScheduler] 📋 Jobs encontrados no config: #{current_jobs.map { |j| j[:name] }.join(', ')}"
        Rails.logger.debug "[CronScheduler] Config completo: #{config.inspect}"  # ⬅️ DEBUG
        
        # Identificar mudanças de forma mais eficiente
        detect_and_apply_changes(current_jobs)
        
        # Executar jobs que estão no horário (já dentro do lock)
        execute_due_jobs
    end



    def load_config
        return { jobs: [] } unless File.exist?(CONFIG_FILE)
        
        begin
        file_content = File.read(CONFIG_FILE)
        config = JSON.parse(file_content, symbolize_names: true)
        
        # ⬇️ CORREÇÃO: Garanta que sempre retorne um hash com :jobs ⬇️
        if config.is_a?(Hash) && config.has_key?(:jobs)
            config
        elsif config.is_a?(Array)
            { jobs: config }  # Se for array, converta para hash
        else
            Rails.logger.error "[CronScheduler] Formato inválido no config: #{config.class}"
            { jobs: [] }
        end
        
        rescue JSON::ParserError => e
        Rails.logger.error "Erro ao parsear #{CONFIG_FILE}: #{e.message}"
        { jobs: [] }
        rescue => e
        Rails.logger.error "Erro ao carregar config: #{e.message}"
        { jobs: [] }
        end
    end

    def detect_and_apply_changes(current_jobs)
        current_names = current_jobs.map { |j| j[:name].to_s }
        existing_names = @scheduled_jobs.keys
        
        Rails.logger.info "[CronScheduler] 🔄 Comparando: atuais=#{current_names}, existentes=#{existing_names}"
        
        # Remover jobs que não existem mais
        (existing_names - current_names).each { |job_name| unschedule_job(job_name) }
        
        # Adicionar/atualizar jobs
        current_jobs.each do |job|
        job_name = job[:name].to_s
        if !@scheduled_jobs[job_name] || job_changed?(@scheduled_jobs[job_name][:config], job)
            Rails.logger.info "[CronScheduler] ➕ Agendando/atualizando: #{job_name}"
            schedule_job(job)
        else
            Rails.logger.debug "[CronScheduler] ⏩ Job unchanged: #{job_name}"
        end
        end
    end

    def job_changed?(old_job, new_job)
        old_job[:cron_expression] != new_job[:cron_expression] ||
        old_job[:job_class] != new_job[:job_class] ||
        old_job[:arguments] != new_job[:arguments] ||
        old_job[:queue] != new_job[:queue]
    end

    def execute_due_jobs
        now = Time.current  # ⬅️ Use Time.current
        Rails.logger.debug "[CronScheduler] Verificando jobs às #{now.strftime('%H:%M:%S %Z')}"
        
        jobs_executados = 0
        @scheduled_jobs.each do |job_name, job_info|
        next unless job_info[:next_execution] && job_info[:next_execution] <= now

        Rails.logger.info "[CronScheduler] ⏰ EXECUTANDO: #{job_name} (agendado para #{job_info[:next_execution].strftime('%H:%M:%S %Z')})"
        
        begin
            enqueue_job_for_execution(job_info[:config])
            jobs_executados += 1
            
            # Reagendar para próxima execução
            next_time = calculate_next_execution(job_info[:cron_expression])
            @scheduled_jobs[job_name][:next_execution] = next_time
            @scheduled_jobs[job_name][:last_execution] = now
            
            Rails.logger.info "[CronScheduler] ✅ #{job_name} executado. Próximo: #{next_time.strftime('%H:%M:%S %Z')}"
        rescue => e
            Rails.logger.error "[CronScheduler] ❌ Erro ao executar #{job_name}: #{e.message}"
        end
        end
        
        Rails.logger.debug "[CronScheduler] #{jobs_executados} jobs executados nesta verificação"
    end

    def calculate_next_execution(cron_expression)
        fugit_cron = Fugit::Cron.parse(cron_expression)
        
        unless fugit_cron
        Rails.logger.error "[CronScheduler] Expressão cron inválida: #{cron_expression}"
        return Time.current + 24.hours
        end
        
        # ⬇️ USE Time.current (que respeita config.time_zone) ⬇️
        next_time = fugit_cron.next_time(Time.current)
        Rails.logger.debug "[CronScheduler] Fugit: '#{cron_expression}' -> #{next_time} (timezone: #{Time.zone.name})"
        next_time
        
    rescue => e
        Rails.logger.error "[CronScheduler] Erro no cron expression '#{cron_expression}': #{e.message}"
        Time.current + 24.hours
    end


    # app/services/cron_scheduler.rb
    private

    def enqueue_job_for_execution(job_config)
        # ⬇️ ESTRUTURA COMPATÍVEL COM REDIS QUEUE WORKER ⬇️
        job_data = {
        job_class: job_config[:job_class],
        arguments: job_config[:arguments] || [],
        queue: job_config[:queue] || "default",
        job_id: "cron_#{job_config[:name]}_#{Time.now.to_i}_#{SecureRandom.hex(4)}",
        
        # ⬇️ METADADOS ESPECÍFICOS PARA JOBS CRON ⬇️
        cron_expression: job_config[:cron_expression],
        cron_uuid: SecureRandom.uuid,
        cron_name: job_config[:name],
        scheduled_at: Time.now.iso8601,
        retry_count: 0,
        enqueued_at: Time.now.iso8601
        }
        
        queue_name = job_config[:queue] || "default"
        
        # ⬇️ CORREÇÃO: Use a chave correta para o RedisQueueWorker ⬇️
        redis_key = "#{REDIS_NAMESPACE}queue:#{queue_name}"  # SEM "queue:" extra!
        
        # ⬇️ ENFILEIRAR NO REDIS ⬇️
        with_redis do |conn|
        conn.rpush(redis_key, job_data.to_json)
        Rails.logger.info "[CronScheduler] Job #{job_config[:name]} enfileirado na chave: #{redis_key}"
        end
        
    rescue => e
        Rails.logger.error "[CronScheduler] Erro ao enfileirar #{job_config[:name]}: #{e.message}"
    end


    def schedule_job(job_config)
        job_name = job_config[:name].to_s
        cron_expression = job_config[:cron_expression]
        
        begin
        next_time = calculate_next_execution(cron_expression)
        @scheduled_jobs[job_name] = {
            config: job_config,
            cron_expression: cron_expression,
            next_execution: next_time,
            last_execution: nil
        }
        
        Rails.logger.info "[CronScheduler] 📅 Agendado #{job_name} para #{next_time.strftime('%H:%M:%S')} (cron: #{cron_expression})"
        rescue => e
        Rails.logger.error "[CronScheduler] Erro ao agendar #{job_name}: #{e.message}"
        end
    end

    def unschedule_job(job_name)
        @scheduled_jobs.delete(job_name)
        Rails.logger.info "[CronScheduler] Removido agendamento para #{job_name}"
    end

    def load_config
        return { jobs: [] } unless File.exist?(CONFIG_FILE)
        
        begin
        file_content = File.read(CONFIG_FILE)
        JSON.parse(file_content, symbolize_names: true) || { jobs: [] }
        rescue JSON::ParserError => e
        Rails.logger.error "Erro ao parsear #{CONFIG_FILE}: #{e.message}"
        { jobs: [] }
        rescue => e
        Rails.logger.error "Erro ao carregar config: #{e.message}"
        { jobs: [] }
        end
    end

    def with_redis
        Rails.cache.redis.with { |conn| yield conn }
    rescue Redis::BaseConnectionError => e
        Rails.logger.error "Redis connection error: #{e.message}"
        raise
    end

    def setup_signal_handlers
        # ⬇️ Use $stdout.puts em vez de Rails.logger dentro do trap ⬇️
        Signal.trap("TERM") do
        $stdout.puts "[CronScheduler] Sinal TERM recebido, iniciando shutdown..."
        @running.make_false  # ⬅️ Use make_false
        end
        
        Signal.trap("INT") do
        $stdout.puts "[CronScheduler] Sinal INT recebido, iniciando shutdown..." 
        @running.make_false  # ⬅️ Use make_false
        end
    end

    # Remover métodos de classe antigos não utilizados
    class << self
        undef_method :scheduled_key, :cron_jobs_registry_key, :schedule_all_jobs, 
                    :clear_existing_config_jobs if method_defined?(:schedule_all_jobs)
    end    





    # Adicione métodos de classe para configuração
    class << self
      def config_file=(path)
        @config_file = path
      end
      
      def config_file
        @config_file || CONFIG_FILE
      end
      
      def redis_namespace=(namespace)
        @redis_namespace = namespace
      end
      
      def redis_namespace
        @redis_namespace || REDIS_NAMESPACE
      end
    end
  end
end