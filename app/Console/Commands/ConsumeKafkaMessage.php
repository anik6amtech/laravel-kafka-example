<?php

namespace App\Console\Commands;

use App\Services\KafkaService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;

class ConsumeKafkaMessage extends Command
{
    protected $signature = 'kafka:consume {--handler=DefaultKafkaHandler} {--topic=test_topic}';
    protected $description = 'Consume messages from a Kafka topic';

    public function handle(KafkaService $kafkaService)
    {
        try {
            // Get the handler name from the options, default to DefaultKafkaHandler
            $handlerName = $this->option('handler');
            $topic = $this->option('topic');

            // Dynamically resolve the handler class
            $handlerClass = "App\\Handlers\\{$handlerName}";

            if (!class_exists($handlerClass)) {
                $this->error("Handler class '{$handlerClass}' not found.");
                return;
            }

            // Instantiate the handler
            $handler = new $handlerClass();

            // Call the Kafka service to consume messages
            $kafkaService->consumeMessages($handler, $topic);

            $this->info("Started consuming messages from topic '{$topic}' with handler '{$handlerClass}'");
        } catch (\Exception $e) {
            $this->error("Error occurred while consuming messages: " . $e->getMessage());
            Log::error("Kafka consumer error", [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
        }
    }
}
