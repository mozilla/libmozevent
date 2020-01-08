# -*- coding: utf-8 -*-
import math
import os
import pickle
import time

import structlog

from libmozevent.utils import AsyncRedis

logger = structlog.get_logger(__name__)


class EphemeralStorage:
    @classmethod
    async def create(cls, name, expiration):
        self = EphemeralStorage()
        self.name = name
        self.expiration = expiration
        self.cache = {}
        self.redis_enabled = "REDIS_URL" in os.environ
        logger.info("Redis support", enabled=self.redis_enabled and "yes" or "no")

        if self.redis_enabled:
            async with AsyncRedis() as redis:
                # Remove all expired keys from our sorted set.
                await redis.zremrangebyscore(
                    self.name, min=-math.inf, max=int(time.time())
                )
                # Load all live keys from our sorted set.
                keys = await redis.zrangebyscore(
                    self.name, min=int(time.time()), max=math.inf
                )
                for key in keys:
                    key = key.decode("utf-8")
                    self.cache[key] = pickle.loads(
                        await redis.get(f"{self.name}:{key}")
                    )

        return self

    def __len__(self):
        return len(self.cache)

    def get(self, key):
        return self.cache[key]

    async def set(self, key, value):
        self.cache[key] = value

        if self.redis_enabled:
            async with AsyncRedis() as redis:
                await redis.expire(self.name, self.expiration)
                await redis.set(
                    f"{self.name}:{key}", pickle.dumps(value), expire=self.expiration
                )
                await redis.zadd(self.name, int(time.time()) + self.expiration, key)

    async def rem(self, key):
        del self.cache[key]

        if self.redis_enabled:
            async with AsyncRedis() as redis:
                await redis.zrem(self.name, key)
