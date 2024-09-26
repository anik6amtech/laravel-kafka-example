<?php

namespace App\Services;

use Junges\Kafka\Facades\Kafka;
use Exception;
use Junges\Kafka\Contracts\Handler;

class KafkaService
{
    protected $brokers;
    protected $topic;

    public function __construct($topic = null, $brokers = null)
    {
        $this->brokers = $brokers ?? env('KAFKA_BROKERS');
        $this->topic = $topic ?? env('KAFKA_DEFAULT_TOPIC');
    }

    // Producer method to send a message
    public function produceMessage(string $message, array $options = [], string $eventType = 'default_event')
    {
        try {
            Kafka::publish($this->brokers)
                ->onTopic($this->topic)
                ->withConfigOptions($options)
                ->withBodyKey('data', $message)
                ->withBodyKey('event_type', $eventType)
                ->send();
        } catch (Exception $e) {
            throw new Exception('Error sending Kafka message: ' . $e->getMessage());
        }
    }

    // Consumer method to consume messages
    public function consumeMessages(Handler $handler, string $topic): void
    {
        // Wrap the single topic in an array
        $topics = [$topic];

        // Consume messages using the provided handler and topics
        $consumer = Kafka::consumer($topics)
            ->withBrokers(env('KAFKA_BROKERS'))
            ->withHandler($handler)
            ->withAutoCommit()
            ->build();

        $consumer->consume();
    }

}
