require 'gorgon/worker_manager_forker'

describe Gorgon::WorkerManagerForker do
  describe "#process" do
    let(:exchange) { double("GorgonBunny Exchange") }
    let(:logger)   { double("Logger") }
    let(:stdin)    { double("Standard Input", :write => nil, :close => nil) }
    let(:stdout)   { double("Standard Output") }
    let(:stderr)   { double("Standard Error") }
    let(:job) { double("Job Definition") }

    let(:worker_manager_forker) { Gorgon::WorkerManagerForker.new('/path/to/config', exchange, logger) }

    before do
      Open4.should_receive(:popen4).with("gorgon manage_workers").and_return([1, stdin, stdout, stderr])
      logger.should_receive(:log).with("Forking Worker Manager...").and_return(anything)
      logger.should_receive(:log).with("Worker Manager 1 finished").and_return(anything)
    end

    context "successful fork" do
      it "returns exit status" do
        process_status = double("Process Status", :exitstatus => 0)
        Process.should_receive(:waitpid2).with(1).and_return([0, process_status])
        worker_manager_forker.process(job).should eq(0)
      end
    end

    context "unsuccessful fork" do
      it "reports crash" do
        process_status = double("Process Status", :exitstatus => 2)
        Process.should_receive(:waitpid2).with(1).and_return([0, process_status])
        logger.should_receive(:log_error).with("Worker Manager 1 crashed with exit status 2!").and_return(anything)
        logger.should_receive(:log_error).with("Process output:\nmessage").and_return(anything)
        worker_manager_forker.should_receive(:report_crash).
          with(exchange, out_file: Gorgon::WorkerManager::STDOUT_FILE,
                         err_file: Gorgon::WorkerManager::STDERR_FILE,
                         footer_text: Gorgon::WorkerManagerForker::ERROR_FOOTER_TEXT).
          and_return("message")

        worker_manager_forker.process(job).should eq(2)
      end
    end
  end
end
