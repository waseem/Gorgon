require "gorgon/configuration"
require "gorgon/amqp_service"
require 'gorgon/callback_handler'
require "gorgon/g_logger"
require 'gorgon/job_definition'

require "uuidtools"
require "awesome_print"
require "socket"
require "benchmark"

module TestRunner
  def self.run_file filename, test_runner
    start_t = Time.now

    begin
      failures = test_runner.run_file(filename)
      length = Time.now - start_t

      if failures.empty?
        results = {:failures => [], :type => :pass, :runner => test_runner.runner, :time => length}
      else
        results = {:failures => failures, :type => :fail, :runner => test_runner.runner,
                   :time => length}
      end
    rescue Exception => e
      results = {:failures => ["Exception: #{e.message}\n#{e.backtrace.join("\n")}"], :type => :crash, :time => (Time.now - start_t)}
    end
    return results
  end
end

module Gorgon
  class Worker
    attr_reader :logger

    class << self
      def build(worker_id, config)
        redirect_output_to_files worker_id

        payload = Yajl::Parser.new(:symbolize_keys => true).parse($stdin.read)
        job_definition = JobDefinition.new(payload)

        connection_config = config[:connection]
        amqp = AmqpService.new connection_config

        callback_handler = CallbackHandler.new(job_definition.callbacks)

        ENV["GORGON_WORKER_ID"] = worker_id.to_s

        params = {
          :amqp => amqp,
          :file_queue_name => job_definition.file_queue_name,
          :reply_exchange_name => job_definition.reply_exchange_name,
          :worker_id => worker_id,
          :callback_handler => callback_handler,
          :log_file => config[:log_file]
        }

        new(params)
      end

      def output_file id, stream
        "/tmp/gorgon-worker-#{id}.#{stream.to_s}"
      end

      def redirect_output_to_files worker_id
        STDOUT.reopen(File.open(output_file(worker_id, :out), 'w'))
        STDOUT.sync = true

        STDERR.reopen(File.open(output_file(worker_id, :err), 'w'))
        STDERR.sync = true
      end
    end

    def initialize(params)
      @logger = GLogger.new(params[:log_file])

      @amqp = params[:amqp]
      @file_queue_name = params[:file_queue_name]
      @reply_exchange_name = params[:reply_exchange_name]
      @worker_id = params[:worker_id]
      @callback_handler = params[:callback_handler]
    end

    def work
      begin
        logger.log "Running before_start callback..."
        register_trap_ints        # do it before calling before_start callback!
        @callback_handler.before_start
        @cleaned = false

        logger.log "Running files ..."
        @amqp.start_worker @file_queue_name, @reply_exchange_name do |queue, exchange|
          while filename = queue.pop
            exchange.publish make_start_message(filename)
            logger.log "Running '#{filename}' with Worker: #{@worker_id}"
            test_results = nil # needed so run_file() inside the Benchmark will use this
            runtime = Benchmark.realtime do
              test_results = run_file(filename)
            end
            exchange.publish make_finish_message(filename, test_results, runtime)
          end
        end
      rescue Exception => e
        clean_up
        raise e                     # So worker manager can catch it
      end
      clean_up
    end

    private

    def clean_up
      return if @cleaned
      logger.log "Running after_complete callback"
      @callback_handler.after_complete
      @cleaned = true
    end

    def run_file(filename)
      framework = test_framework(filename).to_s

      require_runner_code_for framework
      runner_class = get_runner_class_for framework

      TestRunner.run_file(filename, runner_class)
    end

    def test_framework(filename)
      if filename =~ /_spec.rb$/i && defined?(RSpec)
        :rspec
      elsif defined?(MiniTest) && !test_unit_gem?
        :mini_test
      elsif defined?(Test)
        :test_unit
      else
        :unknown
      end
    end

    # Is the user using test-unit gem?
    def test_unit_gem?
      gemfile_lock = "./Gemfile.lock"
      @using_test_unit_gem ||= File.exists?(gemfile_lock) &&
        File.read(gemfile_lock).scan(/\btest-unit/).any?
    end

    def make_start_message(filename)
      {action: :start, hostname: Socket.gethostname, worker_id: @worker_id, filename: filename}
    end

    def make_finish_message(filename, results, runtime)
      {action: :finish, hostname: Socket.gethostname, worker_id: @worker_id, filename: filename, runtime: runtime}.merge(results)
    end

    def require_runner_code_for framework
      ruby_file = framework.to_s + "_runner"
      require_relative ruby_file
    end

    def get_runner_class_for framework
      class_name = camelize(framework) + "Runner"
      Kernel.const_get(class_name)
    end

    def camelize str
      str.split("_").map(&:capitalize).join
    end

    def register_trap_ints
      Signal.trap("INT") { ctrl_c }
      Signal.trap("TERM") { ctrl_c }
    end

    def ctrl_c
      clean_up
      exit
    end
  end
end
