<?php

namespace App\Console\Commands;

use App\Service\KafkaHandler;
use Junges\Kafka\Facades\Kafka;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;

class ConsumeKafkaMessage extends Command
{
    protected $signature = 'kafka:consume';
    protected $description = 'Consume messages from a Kafka topic';

    public function handle()
    {
        try {
            $consumer = Kafka::consumer(['test2'])
                ->withBrokers(env('KAFKA_BROKERS'))
                ->withHandler(new KafkaHandler())

                ->withAutoCommit()
                ->build();

            $this->info("Starting to consume messages from 'test2'...");
            Log::info("Kafka consumer started", ['topic' => 'test2']);

            // Start consuming messages
            $consumer->consume();
        } catch (\Exception $e) {
            $this->error("Error occurred while consuming messages: " . $e->getMessage());
            Log::error("Kafka consumer error", [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
        }
    }
}
