<?php

namespace Rikudou\RedisHelper\Exception;

use InvalidArgumentException;
use Redis;
use Throwable;

final class RedisInvalidTypeException extends InvalidArgumentException
{
    /**
     * RedisInvalidTypeException constructor.
     *
     * @param int            $actualType
     * @param int|string     $expectedType
     * @param Throwable|null $previous
     */
    public function __construct(int $actualType, $expectedType, Throwable $previous = null)
    {
        if (is_int($expectedType)) {
            $expectedType = $this->getHumanFriendlyType($expectedType);
        }

        $message = sprintf(
            "Expected the Redis type '%s' but got '%s'",
            $expectedType,
            $this->getHumanFriendlyType($actualType)
        );
        parent::__construct($message, 0, $previous);
    }

    private function getHumanFriendlyType(int $type): string
    {
        switch ($type) {
            case Redis::REDIS_STRING:
                return 'string';
            case Redis::REDIS_SET:
                return 'set';
            case Redis::REDIS_LIST:
                return 'list';
            case Redis::REDIS_ZSET:
                return 'sorted set';
            case Redis::REDIS_HASH:
                return 'hash';
            case Redis::REDIS_NOT_FOUND:
                return 'other';
            default:
                return 'unknown';
        }
    }
}
