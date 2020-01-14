# -*- coding: utf-8 -*-
import pickle

import structlog

logger = structlog.get_logger(__name__)


class EphemeralStorage:
    def __init__(self, name, expiration, redis_conn=None):
        self.name = name
        self.expiration = expiration
        self.cache = {}
        self.redis_conn = redis_conn
        logger.info("Redis support", enabled="yes" if redis_conn else "no")

    def _redis_key(self, key):
        return f"{self.name}:{key}"

    async def get(self, key):
        if self.redis_conn and key not in self.cache:
            val = await self.redis_conn.get(self._redis_key(key))
            if val is not None:
                self.cache[key] = pickle.loads(val)

        return self.cache[key]

    async def set(self, key, value):
        self.cache[key] = value

        if self.redis_conn:
            await self.redis_conn.expire(self.name, self.expiration)
            await self.redis_conn.set(
                self._redis_key(key), pickle.dumps(value), expire=self.expiration
            )

    async def rem(self, key):
        if self.redis_conn:
            await self.redis_conn.delete(self._redis_key(key))

        try:
            del self.cache[key]
        except KeyError:
            pass
