require "gorgon/worker_manager"
require "gorgon/crash_reporter"

require "open4"
require "socket"

module Gorgon
  class WorkerManagerForker
    include CrashReporter

    ERROR_FOOTER_TEXT = "\n***** See #{WorkerManager::STDERR_FILE} and #{WorkerManager::STDOUT_FILE} at '#{Socket.gethostname}' for complete output *****\n"

    attr_reader :config_filename, :exchange, :logger

    def initialize(config_filename, exchange, logger)
      @config_filename = config_filename
      @exchange        = exchange
      @logger          = logger
    end

    def process(job)
      logger.log "Forking Worker Manager..."
      ENV["GORGON_CONFIG_PATH"] = config_filename

      pid, stdin = Open4::popen4 "gorgon manage_workers"
      stdin.write(job)
      stdin.close

      _, status = Process.waitpid2 pid
      logger.log "Worker Manager #{pid} finished"

      handle_error(status.exitstatus, pid) if status.exitstatus != 0
      status.exitstatus
    end

    private

    def handle_error(exitstatus, pid)
      logger.log_error "Worker Manager #{pid} crashed with exit status #{exitstatus}!"

      message = report_crash(exchange, :out_file    => WorkerManager::STDOUT_FILE,
                                       :err_file    => WorkerManager::STDERR_FILE,
                                       :footer_text => ERROR_FOOTER_TEXT)

      logger.log_error "Process output:\n#{message}"
    end
  end
end
