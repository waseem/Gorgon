require 'gorgon/listener'

describe Gorgon::Listener do
  let(:connection_information) { double }
  let(:queue) { double("GorgonBunny Queue", :bind => nil, :name => "some supposedly unique string") }
  let(:exchange) { double("GorgonBunny Exchange", :publish => nil) }
  let(:bunny) { double("GorgonBunny", :start => nil, :queue => queue, :exchange => exchange) }
  let(:logger) { double("Logger", :log_error => nil, :log => nil) }
  let(:listener) { Gorgon::Listener.new }

  before do
    Gorgon::GLogger.stub(:new).with("logfile.log").and_return(logger)
    GorgonBunny.stub(:new).and_return(bunny)
    Gorgon::Listener.any_instance.stub(:configuration => {:log_file => "logfile.log"})
    Gorgon::Listener.any_instance.stub(:connection_information => connection_information)
  end

  context "initialized" do
    describe "#connect" do
      it "connects" do
        GorgonBunny.should_receive(:new).with(connection_information).and_return(bunny)
        bunny.should_receive(:start)

        listener.connect
      end
    end

    describe "#initialize_personal_job_queue" do
      it "creates the job queue" do
        UUIDTools::UUID.stub(:timestamp_create => "abcd1234")

        bunny.should_receive(:queue).with("job_queue_abcd1234", :auto_delete => true)
        listener.initialize_personal_job_queue
      end

      it "builds job_exchange_name using cluster_id from configuration" do
        Gorgon::Listener.any_instance.stub(:configuration).and_return(:cluster_id => 'cluster5', :log_file => 'logfile.log')
        bunny.should_receive(:exchange).with('gorgon.jobs.cluster5', anything).and_return(exchange)
        listener.initialize_personal_job_queue
      end

      it "binds the exchange to the queue. Uses gorgon.jobs if there is no job_exchange_name in configuration" do
        bunny.should_receive(:exchange).with("gorgon.jobs", :type => :fanout).and_return(exchange)
        queue.should_receive(:bind).with(exchange)
        listener.initialize_personal_job_queue
      end
    end

    describe "#announce_readiness_to_originators" do
      it "publishes data to the originator exchange" do
        originator_exchange = double

        bunny.should_receive(:exchange).with("gorgon.originators", :type => :fanout).and_return(originator_exchange)
        originator_exchange.should_receive(:publish).with(Yajl::Encoder.encode({:listener_queue_name => "some supposedly unique string"}))

        listener.announce_readiness_to_originators
      end
    end

    describe "#poll" do
      let(:empty_queue) { [nil, nil, nil] }
      let(:job_payload) { [nil, nil, Yajl::Encoder.encode({:type => "job_definition"})] }
      before do
        listener.stub(:run_job)
      end

      context "empty queue" do
        before do
          queue.stub(:pop => empty_queue)
        end

        it "checks the job queue" do
          queue.should_receive(:pop).and_return(empty_queue)
          listener.poll
        end

        it "returns false" do
          listener.poll.should be_false
        end
      end

      context "job pending on queue" do
        before do
          queue.stub(:pop => job_payload)
        end

        it "starts a new job when there is a job payload" do
          queue.should_receive(:pop).and_return(job_payload)
          listener.should_receive(:run_job).with({:type => "job_definition"})
          listener.poll
        end

        it "returns true" do
          listener.poll.should be_true
        end
      end

      context "ping message pending on queue" do
        let(:ping_payload) { [nil, nil, Yajl::Encoder.encode({:type => "ping", :reply_exchange_name => "name", :body => {}}) ] }

        before do
          queue.stub(:pop => ping_payload)
          listener.stub(:configuration).and_return({:worker_slots => 3})
       end

        it "publishes ping_response message with Gorgon's version" do
          listener.should_not_receive(:run_job)
          bunny.should_receive(:exchange).with("name", anything).and_return(exchange)
          response = {:type => "ping_response", :hostname => Socket.gethostname,
            :version => Gorgon::VERSION, :worker_slots => 3}
          exchange.should_receive(:publish).with(Yajl::Encoder.encode(response))
          listener.poll
        end
      end

      context "gem_command message pending on queue" do
        let(:command) { "install" }

        let(:payload) {
            {:type => "gem_command", :reply_exchange_name => "name",
              :body => {:command => command}}
        }

        let(:gem_command_handler) { double("GemCommandHandler", :handle => nil)  }
        let(:configuration) { {:worker_slots => 3} }
        before do
          queue.stub(:pop => [nil, nil, Yajl::Encoder.encode(payload)])
          listener.stub(:configuration).and_return(configuration)
        end

        it "calls GemCommandHandler#handle and pass payload" do
          GemCommandHandler.should_receive(:new).with(bunny).and_return gem_command_handler
          gem_command_handler.should_receive(:handle).with payload, configuration
          listener.poll
        end
      end
    end

    describe "#run_job" do
      let(:payload) {{
          :sync => {:source_tree_path => "path/to/source", :exclude => ["log"]}, :callbacks => {:a_callback => "path/to/callback"}
        }}

      let(:syncer) { double("SourceTreeSyncer", :sync => nil, :exclude= => nil, :success? => true,
                          :output => "some output", :errors => "some errors",
                          :remove_temp_dir => nil, :sys_command => "rsync ...")}
      let(:process_status) { double("Process Status", :exitstatus => 0)}
      let(:callback_handler) { double("Callback Handler", :after_sync => nil) }
      let(:worker_manager_forker) { double("Worker Manager Forker") }
      let(:job_definition) { {
        :type => "job_definition",
        :file_queue_name => nil,
        :reply_exchange_name => nil,
      }.merge(payload) }

      before do
        stub_classes
      end

      it "copies source code" do
        worker_manager_forker.should_receive(:process).with(Yajl::Encoder.encode(job_definition))
        SourceTreeSyncer.should_receive(:new).once.
          with(source_tree_path: "path/to/source", exclude: ["log"]).
          and_return(syncer)
        syncer.should_receive(:sync)
        syncer.should_receive(:success?).and_return(true)

        listener.run_job(payload)
      end

      context "syncer#sync fails" do
        before do
          syncer.stub(:success?).and_return false
          syncer.stub(:output).and_return "some output"
          syncer.stub(:errors).and_return "some errors"
          worker_manager_forker.should_not_receive(:process).with(anything)
        end

        it "aborts current job" do
          callback_handler.should_not_receive(:after_sync)
          listener.run_job(payload)
        end

        it "sends message to originator with output and errors from syncer" do
          listener.should_receive(:send_crash_message).with exchange, "some output", "some errors"
          listener.run_job(payload)
        end
      end

      it "remove temp source directory when complete" do
        worker_manager_forker.should_receive(:process).with(Yajl::Encoder.encode(job_definition))
        syncer.should_receive(:remove_temp_dir)
        listener.run_job(payload)
      end

      it "creates a CallbackHandler object using callbacks passed in payload" do
        worker_manager_forker.should_receive(:process).with(Yajl::Encoder.encode(job_definition))
        CallbackHandler.should_receive(:new).once.with({:a_callback => "path/to/callback"}).and_return(callback_handler)
        listener.run_job(payload)
      end

      it "calls after_sync callback" do
        worker_manager_forker.should_receive(:process).with(Yajl::Encoder.encode(job_definition))
        callback_handler.should_receive(:after_sync).once
        listener.run_job(payload)
      end
    end

    private

    def stub_classes
      SourceTreeSyncer.stub(:new).and_return syncer
      CallbackHandler.stub(:new).and_return callback_handler
      Gorgon::WorkerManagerForker.stub(:new).and_return worker_manager_forker
      Socket.stub(:gethostname).and_return("hostname")
    end
  end
end
