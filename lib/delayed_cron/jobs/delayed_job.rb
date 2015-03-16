require 'delayed_job'

module DelayedCron
  module Jobs
    class DelayedJob < Struct.new(:klass, :method_name, :options)

      def self.enqueue_delayed_cron(klass, method_name, options)
        unless scheduled?(klass, method_name)
          options.symbolize_keys!
          ::Delayed::Job.enqueue(
            :payload_object => new(klass, method_name, options),
            :run_at => Time.now + options[:interval],
            :queue => :cron_job
          )
        end
      end

      def self.scheduled?(klass, method_name)
        jobs_count = 0
        ::Delayed::Job.where(:queue => :cron_job).each do |job|
          obj = YAML.load_dj(job.handler)
          if (obj["klass"] == klass && obj["method_name"] == method_name)
            jobs_count += 1
          end
        end
        jobs_count > 1
      end

      def perform
        DelayedCron.process_job(klass, method_name, options)
      end

    end
  end
end
