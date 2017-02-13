require "gorgon/job_definition"
require "gorgon/configuration"
require 'gorgon/source_tree_syncer'
require "gorgon/g_logger"
require "gorgon/callback_handler"
require "gorgon/version"
require "gorgon/worker_manager_forker"
require "gorgon/crash_reporter"
require "gorgon/gem_command_handler"
require 'gorgon/originator_protocol'

require "yajl"
require "gorgon_bunny/lib/gorgon_bunny"
require "awesome_print"
require "tmpdir"
require "socket"

module Gorgon
  class Listener
    include Configuration
    include CrashReporter

    attr_reader :logger

    def initialize
      @listener_config_filename = Dir.pwd + "/gorgon_listener.json"
      @logger = GLogger.new(configuration[:log_file])

      @logger.log "Listener #{Gorgon::VERSION} initializing"
      connect
      initialize_personal_job_queue
      announce_readiness_to_originators
    end

    def listen
      at_exit_hook
      logger.log "Waiting for jobs..."
      while true
        sleep 2 unless poll
      end
    end

    def connect
      @bunny = GorgonBunny.new(connection_information)
      @bunny.start
    end

    def initialize_personal_job_queue
      @job_queue = @bunny.queue("job_queue_" + UUIDTools::UUID.timestamp_create.to_s, :auto_delete => true)
      exchange = @bunny.exchange(job_exchange_name, :type => :fanout)
      @job_queue.bind(exchange)
    end

    def announce_readiness_to_originators
      exchange = @bunny.exchange(originator_exchange_name, :type => :fanout)
      data = {:listener_queue_name => @job_queue.name}
      exchange.publish(Yajl::Encoder.encode(data))
    end

    def poll
      message = @job_queue.pop
      return false if message == [nil, nil, nil]
      logger.log "Received: #{message}"

      payload = message[2]

      handle_request payload

      logger.log "Waiting for more jobs..."
      return true
    end

    def handle_request json_payload
      payload = Yajl::Parser.new(:symbolize_keys => true).parse(json_payload)

      case payload[:type]
      when "job_definition"
        run_job(payload)
      when "ping"
        respond_to_ping payload[:reply_exchange_name]
      when "gem_command"
        GemCommandHandler.new(@bunny).handle payload, configuration
      end
    end

    def run_job(payload)
      @job_definition = JobDefinition.new(payload)
      @reply_exchange = @bunny.exchange(@job_definition.reply_exchange_name, :auto_delete => true)

      copy_source_tree(@job_definition.sync)

      if !@syncer.success? || !run_after_sync
        clean_up
        return
      end

      fork_worker_manager

      clean_up
    end

    def at_exit_hook
      at_exit { logger.log "Listener will exit!"}
    end

    private

    def run_after_sync
      logger.log "Running after_sync callback..."
      begin
        callback_handler.after_sync
      rescue Exception => e
        logger.log_error "Exception raised when running after_sync callback_handler. Please, check your script in #{@job_definition.callbacks[:after_sync]}:"
        logger.log_error e.message
        logger.log_error "\n" + e.backtrace.join("\n")

        reply = {:type => :exception,
                 :hostname => Socket.gethostname,
                 :message => "after_sync callback failed. Please, check your script in #{@job_definition.callbacks[:after_sync]}. Message: #{e.message}",
                 :backtrace => e.backtrace.join("\n")
        }
        @reply_exchange.publish(Yajl::Encoder.encode(reply))
        return false
      end
      true
    end

    def callback_handler
      @callback_handler ||= CallbackHandler.new(@job_definition.callbacks)
    end

    def copy_source_tree(sync_configuration)
      logger.log "Downloading source tree to temp directory..."
      @syncer = SourceTreeSyncer.new sync_configuration
      @syncer.sync
      if @syncer.success?
        logger.log "Command '#{@syncer.sys_command}' completed successfully."
      else
        send_crash_message @reply_exchange, @syncer.output, @syncer.errors
        logger.log_error "Command '#{@syncer.sys_command}' failed!"
        logger.log_error "Stdout:\n#{@syncer.output}"
        logger.log_error "Stderr:\n#{@syncer.errors}"
      end
    end

    def clean_up
      @syncer.remove_temp_dir
    end

    def fork_worker_manager
      Gorgon::WorkerManagerForker.new(@listener_config_filename, @reply_exchange, logger).process(@job_definition.to_json)
    end

    def respond_to_ping reply_exchange_name
      reply = {:type => "ping_response", :hostname => Socket.gethostname,
               :version => Gorgon::VERSION, :worker_slots => configuration[:worker_slots]}
      publish_to reply_exchange_name, reply
    end

    def publish_to reply_exchange_name, message
      reply_exchange = @bunny.exchange(reply_exchange_name, :auto_delete => true)

      logger.log "Sending #{message}"
      reply_exchange.publish(Yajl::Encoder.encode(message))
    end

    def job_exchange_name
      OriginatorProtocol.job_exchange_name(configuration.fetch(:cluster_id, nil))
    end

    def originator_exchange_name
      OriginatorProtocol.originator_exchange_name(configuration.fetch(:cluster_id, nil))
    end

    def connection_information
      configuration[:connection]
    end

    def configuration
      @configuration ||= load_configuration_from_file("gorgon_listener.json")
    end
  end
end
