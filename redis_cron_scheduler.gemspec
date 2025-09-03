# redis_cron_scheduler.gemspec
# -*- encoding: utf-8 -*-

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'redis_cron_scheduler/version'

Gem::Specification.new do |spec|
  spec.name          = "redis_cron_scheduler"
  spec.version       = RedisCronScheduler::VERSION
  spec.authors       = ["Guy Novaes"]
  spec.email         = ["guynovaes@gmail.com"]

  spec.summary       = "Redis-based cron scheduler and queue worker"
  spec.description   = "A distributed cron scheduler and background job worker using Redis with web dashboard"
  spec.homepage      = "https://github.com/guynovaes/redis_cron_scheduler"
  spec.license       = "MIT"

  # Especifica arquivos manualmente (elimina warning do git)
  spec.files         = Dir[
    "lib/**/*",
    "app/**/*",
    "config/**/*",
    "README.md",
    "LICENSE.txt",
    "CHANGELOG.md"
  ]
  spec.require_paths = ["lib"]

  spec.required_ruby_version = '>= 2.7.0'

  # Dependências com versionamento semântico
  spec.add_runtime_dependency "rails", "~> 8.0.2", ">= 8.0.2.1"
  spec.add_runtime_dependency "concurrent-ruby", "~> 1.1", ">= 1.1.9"
  spec.add_runtime_dependency "fugit", '~> 1.11.0'
  spec.add_runtime_dependency "redis", '~> 5.4', '>= 5.4.1'

  spec.add_development_dependency "bundler", ">= 1.15.0"
  spec.add_development_dependency "rake", "~> 13.3.0"

end