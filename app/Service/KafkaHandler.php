<?php

namespace App\Service;

use Illuminate\Support\Facades\Log;
use Junges\Kafka\Contracts\ConsumerMessage;
use Exception;

class KafkaHandler
{
    public function __invoke(ConsumerMessage $message)
    {
        try {
            // Get the message body
            $body = $message->getBody();

            // Check if the body is already an array
            if (is_array($body)) {
                $payload = $body;
            } else {
                // If it's a string, try to JSON decode it
                $payload = json_decode($body, true);
                if (json_last_error() !== JSON_ERROR_NONE) {
                    throw new Exception("Invalid JSON in message body: " . json_last_error_msg());
                }
            }

            // Log the received message
            Log::info('Kafka message received', [
                'topic' => $message->getTopicName(),
                'partition' => $message->getPartition(),
                'offset' => $message->getOffset(),
                'key' => $message->getKey(),
                'headers' => $message->getHeaders(),
                'body' => $payload
            ]);

            // Process the message
            $this->processMessage($payload);

        } catch (Exception $e) {
            Log::error('Error processing Kafka message', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'topic' => $message->getTopicName(),
                'partition' => $message->getPartition(),
                'offset' => $message->getOffset(),
                'body' => $message->getBody()  // Log the raw body for debugging
            ]);
        }
    }

    private function processMessage(array $payload)
    {
        if (isset($payload['foo'])) {
            // Process the 'foo' field
            $fooValue = $payload['foo'];
            Log::info("Processed message with foo value", ['foo' => $fooValue]);

            // Add your business logic here
            // For example:
            if ($fooValue === 'test') {
                // Do something specific for test messages
                Log::info("Received a test message");
            } else {
                // Handle other cases
                Log::info("Received a non-test message");
            }
        } else {
            Log::warning('Message payload does not contain expected "foo" field', ['payload' => $payload]);
        }
    }
}
