require 'rails'
require 'redis_cron_scheduler/web/engine'  # <-- garante que a engine existe

module RedisCronScheduler
  class Railtie < Rails::Railtie
    initializer "redis_cron_scheduler.setup" do |app|
      app.config.after_initialize do
        if ENV['REDIS_CRON_AUTO_START'] != 'false'
          RedisCronScheduler.start_scheduler
          RedisCronScheduler.start_workers
        end
      end
    end

#    initializer "redis_cron_scheduler.routes" do |app|
#      app.routes.append do
#        mount RedisCronScheduler::Web::Engine => '/redis_cron'
#      end
#    end

    rake_tasks do
      load File.expand_path('../tasks/redis_cron_scheduler.rake', __FILE__)
    end
  end
end
