<?php

namespace App\Console\Commands;

use Junges\Kafka\Facades\Kafka;
use Illuminate\Console\Command;

class ProduceKafkaMessage extends Command
{
    protected $signature = 'kafka:produce {message} {--event_type=default_event}';
    protected $description = 'Produce a message to Kafka';

    public function handle()
    {
        $message = $this->argument('message');
        $eventType = $this->option('event_type');

        try {



            Kafka::publish(env('KAFKA_BROKERS'))->onTopic('test2')
                ->withConfigOptions([
                    'socket.timeout.ms' => 50,
                ])
                ->withBodyKey('data', $message)
                ->withBodyKey('event_type', $eventType)
                ->withHeaders([
                    'data-header' => 'data-value'
                ])
                ->send();

            $this->info("Message sent to Kafka topic 'test2'");
            $this->info("Message: $message");
            $this->info("Event Type: $eventType");
        } catch (\Exception $e) {
            $this->error('Error sending message to Kafka: ' . $e->getMessage());
        }
    }
}
