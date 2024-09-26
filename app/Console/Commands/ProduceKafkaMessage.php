<?php

namespace App\Console\Commands;

use App\Services\KafkaService;
use Illuminate\Console\Command;

class ProduceKafkaMessage extends Command
{
    protected $signature = 'kafka:produce {message} {--event_type=default_event} {--topic=}';
    protected $description = 'Produce a message to Kafka';

    public function __construct(protected KafkaService $kafkaService)
    {
        parent::__construct();
    }

    public function handle()
    {
        $message = $this->argument('message');
        $eventType = $this->option('event_type');
        $topic = $this->option('topic') ?? env('KAFKA_DEFAULT_TOPIC');

        try {
            $this->kafkaService->produceMessage($message, [], $eventType);
            $this->info("Message sent to Kafka topic '{$topic}'");
        } catch (\Exception $e) {
            $this->error('Error sending Kafka message: ' . $e->getMessage());
        }
    }
}
