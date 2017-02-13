require 'gorgon/g_logger'

module Gorgon
  class OriginatorLogger
    attr_reader :logger

    def initialize(log_file)
      @logger = GLogger.new(log_file)
    end

    def log_message(payload)
      if payload[:action] == "start"
        logger.log("Started running '#{payload[:filename]}' at '#{payload[:hostname]}:#{payload[:worker_id]}'")
      elsif payload[:action] == "finish"
        print_finish(payload)
      elsif payload[:type] == "crash" || payload[:type] == "exception"
        # TODO: improve logging of these messages
        logger.log(payload)
      else # to be removed
        ap payload
      end
    end

    private

    def print_finish(payload)
      msg = "Finished running '#{payload[:filename]}' at '#{payload[:hostname]}:#{payload[:worker_id]}'"
      msg << failure_message(payload[:failures]) if payload[:type] == "fail"
      logger.log msg
    end

    def failure_message(failures)
      msg = []
      failures.each do |failure|
        msg << failure
      end
      msg << ''
      msg.join("\n")
    end
  end
end
