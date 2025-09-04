## Web Dashboard

Acesse o dashboard em: `http://localhost:3000/redis_cron`

### Necessário:

```bash
export REDIS_NAMESPACE="myapp::"
export REDIS_WORKER_COUNT=1
export REDIS_CRON_AUTO_START=true
```

## Necessário incluir no environment o cache_store

```ruby
config.cache_store = :redis_cache_store, {
  url: "rediss://#{ENV['elasticache_valkey_username']}:#{ENV['elasticache_valkey_password']}@#{ENV['REDIS_SERVER']}:6379/0",
  ssl_params: { verify_mode: OpenSSL::SSL::VERIFY_NONE },
  namespace: ENV['REDIS_NAMESPACE']
}
```

## Incluir no config/routes.rb

```ruby
scope "/redis_cron", module: "redis_cron_scheduler/web", as: "redis_cron_scheduler_web" do
  root to: "redis_jobs#index"
  resources :redis_jobs, only: [:index]
end
```

[![Gem Version](https://badge.fury.io/rb/redis_cron_scheduler.svg?icon=si%3Arubygems)](https://badge.fury.io/rb/redis_cron_scheduler)

### Features do Dashboard:
- 📊 Estatísticas em tempo real
- 👀 Visualização de todas as filas
- ⏰ Jobs agendados
- 🔄 Jobs em retry
- 💀 Dead jobs
- 🏃 Jobs em execução
- ⚡ Ações: retry, delete, retry all, delete all
