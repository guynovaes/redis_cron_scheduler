# lib/redis_cron_scheduler/web/routes.rb
RedisCronScheduler::Web::Engine.routes.draw do
  get "/" => "redis_jobs#index"
  resources :redis_jobs, only: [:index]
end

