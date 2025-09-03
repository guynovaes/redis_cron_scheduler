# lib/redis_cron_scheduler/web/controllers/redis_jobs_controller.rb
module RedisCronScheduler
  module Web
    class RedisJobsController < ActionController::Base

            def index
                respond_to do |format|
                    format.html do
                        @queues_info = fetch_queues_info
                    end
                    format.json do
                        render json: fetch_queues_info
                    end
                end
            end

            private

            def fetch_queues_info
                redis_namespace = ENV['REDIS_NAMESPACE']
                queues_info = {}
                queues = %w[default mailers critical]

                Rails.cache.redis.with do |conn|
                    queues.each do |queue|
                        queue_key     = "#{redis_namespace}queue:#{queue}"
                        scheduled_key = "#{redis_namespace}schedule:#{queue}"
                        retry_key     = "#{redis_namespace}retry:#{queue}"
                        running_key   = "#{redis_namespace}running:#{queue}"
                        dead_key      = "#{redis_namespace}dead:#{queue}"

                        pending_count   = conn.llen(queue_key) || 0
                        scheduled_count = conn.zcard(scheduled_key) || 0
                        retry_count     = conn.zcard(retry_key) || 0
                        dead_count     = conn.zcard(dead_key) || 0

                        # Contar keys de running (prefixo usado no worker)
                        running_keys = conn.keys("#{running_key}:*") || []
                        running_count = running_keys.size

                        queues_info[queue] = {
                        pending: pending_count,
                        scheduled: scheduled_count,
                        retry: retry_count,
                        running: running_count,
                        dead: dead_count

                        }
                    end
                end
                queues_info
            end
        end
    end
end