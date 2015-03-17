require 'delayed_job'

module DelayedCron
  module Jobs
    class DelayedJob < Struct.new(:klass, :method_name, :options)

      def self.enqueue_delayed_cron(klass, method_name, options)
				first_run = options.delete(:first_run)
        unless scheduled?(klass, method_name, first_run)
          options.symbolize_keys!
          ::Delayed::Job.enqueue(
            :payload_object => new(klass, method_name, options),
            :run_at => Time.now + options[:interval],
            :queue => :cron_job
          )
        end
      end

      def self.scheduled?(klass, method_name, first_run)
				allowed_jobs_count = first_run ? 0 : 1
        ::Delayed::Job.where(:queue => :cron_job).each do |job|
          obj = YAML.load_dj(job.handler)
          if (obj["klass"] == klass && obj["method_name"] == method_name)
						allowed_jobs_count -= 1
					end
				end
				allowed_jobs_count >= 0
      end

      def perform
        DelayedCron.process_job(klass, method_name, options)
      end

    end
  end
end
