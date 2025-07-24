# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import contextvars
import os
import signal
from typing import Iterable

import structlog
from redis import asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import (
    ConnectionError,
    ExecAbortError,
    PubSubError,
    RedisError,
    ResponseError,
    TimeoutError,
    WatchError,
)

log = structlog.get_logger(__name__)


def run_tasks(awaitables: Iterable, bus_to_restore=None):
    """
    Helper to run tasks concurrently.

    When a task raises an exception, the whole stack stops and the exception is raised.
    When a SIGTERM is received, the whole stack stops and a asyncio.CancelledError is raised.

    In case a SIGTERM is received and bus_to_restore is defined, Redis messages currently
    processed by sequential tasks will be put back at the top of their original queue.
    """

    try:
        # Create a task grouping all awaitables and running them concurrently
        tasks_group = asyncio.gather(*awaitables)
    except TypeError:
        raise TypeError(
            "Could not run tasks: awaitable parameter must only contain awaitable functions"
        )

    def handle_sigterm(*args, **kwargs):
        """
        Stop all tasks when receiving a SIGTERM.
        This may particularly happen on Heroku dynos, been stopped at least once a day.
        """
        log.warning("SIGTERM signal has been received. Stopping all running tasks…")
        tasks_group.cancel()

    event_loop = asyncio.get_event_loop()
    event_loop.add_signal_handler(signal.SIGTERM, handle_sigterm)

    async def _run():
        try:
            await tasks_group
        except Exception as e:
            log.error("Failure while running async tasks", error=str(e), exc_info=True)
            # When ANY exception from one of the awaitables
            # make sure the other awaitables are cancelled
            tasks_group.cancel()

    try:
        event_loop.run_until_complete(_run())
    except asyncio.CancelledError as e:
        # Tasks have been cancelled intentionally, restore messages in Redis queues
        if bus_to_restore is not None:
            event_loop.run_until_complete(bus_to_restore.restore_redis_messages())
        raise e


class AsyncRedis(object):
    """
    Async context manager to create a redis connection
    """

    redis: contextvars.ContextVar[aioredis.Redis] = contextvars.ContextVar(
        "redis-server"
    )

    @staticmethod
    async def connect():
        if AsyncRedis.redis.get(None) is None:
            retry_on_error = [
                ConnectionError,
                ExecAbortError,
                PubSubError,
                RedisError,
                ResponseError,
                TimeoutError,
                WatchError,
            ]

            RETRIES = 5
            AsyncRedis.redis.set(
                await aioredis.from_url(
                    os.environ["REDIS_TLS_URL"],
                    decode_responses=False,
                    ssl_cert_reqs=None,
                    health_check_interval=30,
                    retry_on_error=retry_on_error,
                    retry=Retry(ExponentialBackoff(cap=7, base=1), RETRIES),
                )
                if os.getenv("REDIS_TLS_URL", None) is not None
                else await aioredis.from_url(
                    os.environ["REDIS_URL"],
                    decode_responses=False,
                    health_check_interval=30,
                    retry_on_error=retry_on_error,
                    retry=Retry(ExponentialBackoff(cap=7, base=1), RETRIES),
                )
            )
        return AsyncRedis.redis.get()
