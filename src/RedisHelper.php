<?php

namespace Rikudou\RedisHelper;

use InvalidArgumentException;
use Redis;
use Rikudou\RedisHelper\Exception\InvalidTypeException;
use Rikudou\RedisHelper\Exception\RedisException;
use Rikudou\RedisHelper\Exception\RedisInvalidTypeException;
use Rikudou\RedisHelper\Exception\RedisKeyNotFoundException;

final class RedisHelper
{
    /**
     * @var Redis
     */
    private $redis;

    public function __construct(Redis $redis)
    {
        $this->redis = $redis;
    }

    public function exists(string $key): bool
    {
        return !!$this->redis->exists($key);
    }

    public function delete(string $key): void
    {
        if (!$this->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }
        $this->redis->del($key);
    }

    public function getType(string $key): int
    {
        if (!$this->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }

        return $this->redis->type($key);
    }

    public function getString(string $key): string
    {
        if (!$this->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }

        if ($this->getType($key) !== Redis::REDIS_STRING) {
            throw new RedisInvalidTypeException($this->getType($key), Redis::REDIS_STRING);
        }

        return $this->redis->get($key);
    }

    public function getInt(string $key): int
    {
        $value = $this->getString($key);
        if (!is_numeric($value)) {
            throw new InvalidTypeException('The value is not a number');
        }

        if (strval(intval($value)) !== $value) {
            throw new InvalidTypeException('The value is not an integer');
        }

        return intval($value);
    }

    public function getFloat(string $key): float
    {
        $value = $this->getString($key);
        if (!is_numeric($value)) {
            throw new InvalidTypeException('The value is not a number');
        }

        return floatval($value);
    }

    public function getBoolean(string $key): bool
    {
        return !!$this->getString($key);
    }

    public function getHash(string $key): array
    {
        if (!$this->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }

        if ($this->getType($key) !== Redis::REDIS_HASH) {
            throw new RedisInvalidTypeException($this->getType($key), Redis::REDIS_HASH);
        }

        return $this->redis->hGetAll($key);
    }

    public function getList(string $key): array
    {
        if (!$this->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }

        if ($this->getType($key) !== Redis::REDIS_LIST) {
            throw new RedisInvalidTypeException($this->getType($key), Redis::REDIS_LIST);
        }

        $result = [];
        $length = $this->redis->lSize($key);
        for ($i = 0; $i < $length; $i++) {
            $result[] = $this->redis->lIndex($key, $i);
        }

        return $result;
    }

    public function getSet(string $key): array
    {
        if (!$this->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }

        if ($this->getType($key) !== Redis::REDIS_SET) {
            throw new RedisInvalidTypeException($this->getType($key), Redis::REDIS_SET);
        }

        return $this->redis->sMembers($key);
    }

    public function getSortedSet(string $key): array
    {
        if (!$this->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }

        if ($this->getType($key) !== Redis::REDIS_ZSET) {
            throw new RedisInvalidTypeException($this->getType($key), Redis::REDIS_ZSET);
        }

        return $this->redis->zRange($key, 0, -1);
    }

    public function getArray(string $key): array
    {
        if (!$this->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }

        switch ($this->getType($key)) {
            case Redis::REDIS_LIST:
                return $this->getList($key);
            case Redis::REDIS_HASH:
                return $this->getHash($key);
            case Redis::REDIS_SET:
                return $this->getSet($key);
            case Redis::REDIS_ZSET:
                return $this->getSortedSet($key);
            default:
                throw new RedisInvalidTypeException($this->getType($key), "list' or 'hash' or 'set' or 'sorted set");
        }
    }

    /**
     * @param string $key
     *
     * @return array|string
     */
    public function get(string $key)
    {
        if (!$this->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }

        switch ($this->getType($key)) {
            case Redis::REDIS_LIST:
            case Redis::REDIS_HASH:
            case Redis::REDIS_SET:
            case Redis::REDIS_ZSET:
                return $this->getArray($key);
            case Redis::REDIS_STRING:
                return $this->getString($key);
            default:
                throw new RedisInvalidTypeException($this->getType($key), "list' or 'hash' or 'set' or 'sorted set' or 'string");
        }
    }

    public function setTtl(string $key, int $ttl)
    {
        if (!$this->redis->exists($key)) {
            throw new RedisKeyNotFoundException($key);
        }

        $this->redis->expire($key, $ttl);
    }

    public function setString(string $key, string $value, ?int $ttl = null): void
    {
        if (!$this->redis->set($key, $value)) {
            throw new RedisException('Could not save value to Redis');
        }
        if ($ttl) {
            $this->setTtl($key, $ttl);
        }
    }

    public function setInt(string $key, int $value, ?int $ttl = null): void
    {
        $this->setString($key, strval($value), $ttl);
    }

    public function setFloat(string $key, float $value, ?int $ttl = null): void
    {
        $this->setString($key, strval($value), $ttl);
    }

    public function setBoolean(string $key, bool $value, ?int $ttl = null): void
    {
        $this->setString($key, $value ? '1' : '0', $ttl);
    }

    public function setHash(string $key, array $value, ?int $ttl = null): void
    {
        if ($this->exists($key)) {
            $this->delete($key);
        }
        if (!$this->redis->hMSet($key, $value)) {
            throw new RedisException('Could not save value to Redis');
        }
        if ($ttl) {
            $this->setTtl($key, $ttl);
        }
    }

    public function setList(string $key, array $value, ?int $ttl = null): void
    {
        if ($this->exists($key)) {
            $this->delete($key);
        }

        if (!$this->redis->lPush($key, ...$value)) {
            throw new RedisException('Could not save value to Redis');
        }

        if ($ttl) {
            $this->setTtl($key, $ttl);
        }
    }

    public function setSet(string $key, array $value, ?int $ttl = null): void
    {
        if ($this->exists($key)) {
            $this->delete($key);
        }

        if (!$this->redis->sAdd($key, ...$value)) {
            throw new RedisException('Could not save value to Redis');
        }

        if ($ttl) {
            $this->setTtl($key, $ttl);
        }
    }

    public function setSortedSet(string $key, array $value, ?int $ttl = null): void
    {
        if ($this->exists($key)) {
            $this->delete($key);
        }

        foreach ($value as $score => $valueToSave) {
            if (!is_int($score)) {
                $score = 0;
            }
            $this->redis->zAdd($key, $score, $valueToSave);
        }

        if ($ttl) {
            $this->setTtl($key, $ttl);
        }
    }

    public function setArray(string $key, array $value, ?int $ttl = null): void
    {
        if ($this->exists($key)) {
            $this->delete($key);
        }

        $allNumeric = true;
        $previousKey = null;
        foreach ($value as $arrayKey => $arrayValue) {
            if (!$allNumeric) {
                break;
            }
            if (!is_int($arrayKey)) {
                $allNumeric = false;
            }
            if (is_null($previousKey) && $arrayKey !== 0) {
                $allNumeric = false;
            }
            if (!is_null($previousKey)) {
                if ($arrayKey !== $previousKey + 1) {
                    $allNumeric = false;
                }
            }

            $previousKey = $arrayKey;
        }

        if ($allNumeric) {
            $this->setList($key, $value, $ttl);
        } else {
            $this->setHash($key, $value, $ttl);
        }
    }

    /**
     * @param string                      $key
     * @param array|string|int|float|bool $value
     * @param int|null                    $ttl
     */
    public function set(string $key, $value, ?int $ttl = null)
    {
        if (is_array($value)) {
            $this->setArray($key, $value, $ttl);
        } elseif (is_string($value)) {
            $this->setString($key, $value, $ttl);
        } elseif (is_int($value)) {
            $this->setInt($key, $value, $ttl);
        } elseif (is_float($value)) {
            $this->setFloat($key, $value, $ttl);
        } elseif (is_bool($value)) {
            $this->setBoolean($key, $value, $ttl);
        } else {
            throw new InvalidArgumentException(sprintf("Unexpected type: '%s'", gettype($value)));
        }
    }

    /**
     * @return Redis
     */
    public function getRedis(): Redis
    {
        return $this->redis;
    }
}
