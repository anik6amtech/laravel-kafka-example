<?php

namespace App\Contracts;

use Junges\Kafka\Contracts\ConsumerMessage;

interface KafkaMessageHandlerInterface
{
    public function handleMessage(ConsumerMessage $message): void;
}
