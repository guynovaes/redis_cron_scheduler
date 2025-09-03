require 'rails/engine'

module RedisCronScheduler
  module Web
    class Engine < ::Rails::Engine
      isolate_namespace RedisCronScheduler::Web
    end
  end
end
