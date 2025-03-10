module BunnySubscriber
  module Worker
    def initialize
      @stop_flag = ServerEngine::BlockingFlag.new
    end

    def run
      channel_options = Configuration.instance.channel_options
      bunny_connection
      @conn.start

      @consumer_instances = consumers.map do |consumer|
        channel = @conn.create_channel
        consumer_instance = consumer.new(channel, logger)
        consumer_instance.start
        consumer_instance
      end

      sleep 5 until @stop_flag.set?
    end

    def stop
      logger.info "++++++++Stopping Worker......."
      unless @stop_flag.set?
        @consumer_instances.each(&:stop)
        @conn.close
      end
      @stop_flag.set!
    end

    def bunny_connection
      options = Configuration.instance.bunny_options.to_h
      @conn = Bunny.new(options)
    end

    def consumers
      consumer_options = Configuration.instance.consumer_options
      string_classes = consumer_options.consumer_classes ||
                       Consumer::CLASSES
      string_classes.map do |consumer_string_class|
        if consumer_string_class.is_a? Class
          consumer_string_class
        else
          Object.const_get(consumer_string_class)
        end
      end
    end
  end
end
