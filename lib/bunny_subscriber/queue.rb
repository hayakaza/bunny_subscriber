module BunnySubscriber
  class Queue
    attr_reader :channel, :queue_consummer

    def initialize(channel)
      @channel = channel
    end

    def subscribe(consumer)
      queue = create_queue(consumer)
      arguments = subscribe_arguments(consumer)
      @queue_consummer = queue.subscribe(
        manual_ack: true,
        block: false,
        arguments: arguments
      ) do |delivery_info, properties, payload|
        consumer.event_process_around_action(
          delivery_info, properties, payload
        )
      end
    end

    def unsubscribe
      return if @queue_consummer.cancel

      # If can cancel the consumer, try again
      sleep(1)
      unsubscribe
    end

    private

    def create_queue(consumer)
      if consumer.subscriber_options[:queue_name].nil?
        raise ArgumentError, '`queue_name` option is required'
      end

      options = { durable: true }
      if (dl_exchange = consumer.subscriber_options[:dead_letter_exchange])
        options[:arguments] = { 'x-dead-letter-exchange': dl_exchange }
      end

      channel.queue(
        consumer.subscriber_options[:queue_name], options
      )
    end

    def subscribe_arguments
      args = {}

      if (timeout = consumer.subscriber_options[:timeout])
        args['x-consumer-timeout'] = timeout
      end

      if (priority = consumer.subscriber_options[:priority])
        args['x-priority'] = priority
      end

      args
    end
  end
end
