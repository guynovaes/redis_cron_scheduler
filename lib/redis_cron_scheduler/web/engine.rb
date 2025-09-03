# lib/redis_cron_scheduler/web/engine.rb
require "rails/engine"

module RedisCronScheduler
  module Web
    class Engine < ::Rails::Engine
      isolate_namespace RedisCronScheduler::Web

      # garante que o Rails enxergue controllers e views dentro do engine
      paths["app/controllers"] << "lib/redis_cron_scheduler/web/controllers"
      paths["app/views"]       << "lib/redis_cron_scheduler/web/views"
      # 👇 carrega as rotas internas do engine

      initializer "redis_cron_scheduler.web.load_routes" do
        config.paths["config/routes.rb"] = File.expand_path("routes.rb", __dir__)
      end     

    end
  end
end

# 👇 este require é essencial para as rotas do engine serem carregadas
#require_relative "routes"
