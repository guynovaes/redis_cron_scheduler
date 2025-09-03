# lib/tasks/redis_cron_scheduler.rake
namespace :redis_cron do
  desc "Start the cron scheduler"
  task :start_scheduler => :environment do
    RedisCronScheduler.start_scheduler
    puts "Cron scheduler started"
  end

  desc "Start the queue workers"
  task :start_workers => :environment do
    RedisCronScheduler.start_workers
    puts "Queue workers started"
  end

  desc "Start both scheduler and workers"
  task :start_all => :environment do
    Rake::Task['redis_cron:start_scheduler'].invoke
    Rake::Task['redis_cron:start_workers'].invoke
  end
end