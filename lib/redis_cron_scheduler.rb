# lib/redis_cron_scheduler.rb
require "concurrent"
require "fugit"
require "redis"
require "json"
require "rails"

# Carregue ActiveSupport para ter .seconds mesmo sem Rails completo
require "active_support"
require "active_support/core_ext/numeric/time"
require "active_support/core_ext/time/calculations"

require_relative "redis_cron_scheduler/version"
require_relative "redis_cron_scheduler/cron_scheduler"
require_relative "redis_cron_scheduler/redis_queue_worker"
require_relative "redis_cron_scheduler/railtie" if defined?(Rails)

module RedisCronScheduler
  class Error < StandardError; end
  
  def self.start_scheduler
    Thread.new do
      CronScheduler.start
    end
  end

  def self.start_workers
    RedisQueueWorker.start_workers
  end

  # Método para configurar manualmente sem Rails
  def self.configure
    yield self if block_given?
  end
end