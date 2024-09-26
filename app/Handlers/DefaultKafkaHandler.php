<?php

namespace App\Handlers;

use Junges\Kafka\Contracts\Handler;
use Junges\Kafka\Contracts\ConsumerMessage;
use Illuminate\Support\Facades\Log;
use Exception;

class DefaultKafkaHandler implements Handler
{
    public function __invoke(ConsumerMessage $message): void
    {
        try {
            $body = $message->getBody();
            $payload = is_array($body) ? $body : json_decode($body, true);

            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new Exception("Invalid JSON: " . json_last_error_msg());
            }

            Log::info('Kafka message processed', [
                'topic' => $message->getTopicName(),
                'partition' => $message->getPartition(),
                'offset' => $message->getOffset(),
                'payload' => $payload,
            ]);

            // Your business logic here
        } catch (Exception $e) {
            Log::error('Error processing Kafka message', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'message' => $message->getBody(),
            ]);
        }
    }
}
