# -*- coding: utf-8 -*-
import os
import pickle

import structlog

from libmozevent.utils import AsyncRedis

logger = structlog.get_logger(__name__)


class EphemeralStorage:
    def __init__(self, name, expiration):
        self.name = name
        self.expiration = expiration
        self.cache = {}
        self.redis_enabled = "REDIS_URL" in os.environ
        logger.info("Redis support", enabled=self.redis_enabled and "yes" or "no")

    def _redis_key(self, key):
        return f"{self.name}:{key}"

    async def get(self, key):
        if self.redis_enabled and key not in self.cache:
            redis = await AsyncRedis.connect()
            assert redis is not None
            val = await redis.get(self._redis_key(key))
            if val is not None:
                self.cache[key] = pickle.loads(val)

        return self.cache[key]

    async def set(self, key, value):
        self.cache[key] = value

        if self.redis_enabled:
            redis = await AsyncRedis.connect()
            assert redis is not None
            await redis.expire(self.name, self.expiration)
            await redis.set(
                self._redis_key(key),
                pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL),
                ex=self.expiration,
            )

    async def rem(self, key):
        if self.redis_enabled:
            redis = await AsyncRedis.connect()
            assert redis is not None
            await redis.delete(self._redis_key(key))

        try:
            del self.cache[key]
        except KeyError:
            pass
