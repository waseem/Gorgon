require 'gorgon/g_logger'

describe Gorgon::GLogger do
  let(:logger) { double("Logger", :datetime_format= => "") }

  describe "initialization" do
    context "no log file provided" do
      it "does not set logger object" do
        Logger.should_not_receive(:new).with(anything)
        Gorgon::GLogger.new(nil)
      end
    end

    context "logfile is -" do
      it "sets stdout as logger file" do
        Logger.should_receive(:new).with($stdout).and_return(logger)
        logger.should_receive(:datetime_format=).with("%Y-%m-%d %H:%M:%S ").and_return(logger)

        Gorgon::GLogger.new("-")
      end
    end
    context "logfile is provided" do
      it "sents the logfile to logger's file" do
        Logger.should_receive(:new).with("logfile.log", 1, Gorgon::GLogger::SIZE_1_MB).and_return(logger)
        logger.should_receive(:datetime_format=).with("%Y-%m-%d %H:%M:%S ").and_return(logger)

        Gorgon::GLogger.new("logfile.log")
      end
    end
  end

  describe "#log" do

    context "logger is present" do
      it "logs with info level" do
        Logger.should_receive(:new).with("logfile.log", 1, Gorgon::GLogger::SIZE_1_MB).and_return(logger)
        logger.should_receive(:info).with("logged text").and_return(anything)
        g_logger = Gorgon::GLogger.new("logfile.log")
        g_logger.log("logged text")
      end
    end

    context "logger is absent" do
      it "does not log" do
        Logger.should_not_receive(:new)
        logger.should_not_receive(:info)

        g_logger = Gorgon::GLogger.new(nil)
        g_logger.log("logged text")
      end
    end
  end

  describe "#log_error" do
    context "logger is present" do
      it "logs with info level" do
        Logger.should_receive(:new).with("logfile.log", 1, Gorgon::GLogger::SIZE_1_MB).and_return(logger)
        logger.should_receive(:error).with("logged text").and_return(anything)
        g_logger = Gorgon::GLogger.new("logfile.log")
        g_logger.log_error("logged text")
      end
    end

    context "logger is absent" do
      it "does not log" do
        Logger.should_not_receive(:new)
        logger.should_not_receive(:error)

        g_logger = Gorgon::GLogger.new(nil)
        g_logger.log_error("logged text")
      end
    end
  end
end
