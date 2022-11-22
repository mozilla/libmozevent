# -*- coding: utf-8 -*-
import asyncio
import collections
import inspect
import multiprocessing
import os
import pickle
from queue import Empty
from typing import Any
from typing import Callable

import structlog

from libmozevent.utils import AsyncRedis

logger = structlog.get_logger(__name__)

RedisQueue = collections.namedtuple("RedisQueue", "name")


class MessageBus(object):
    """
    Communication bus between processes
    """

    def __init__(self):
        self.queues = {}
        self.redis_enabled = "REDIS_URL" in os.environ
        logger.info("Redis support", enabled=self.redis_enabled and "yes" or "no")
        # Stores currently consumed Redis messages
        self.redis_messages = {}

    def add_queue(
        self, name: str, mp: bool = False, redis: bool = False, maxsize: int = -1
    ):
        """
        Create a new queue on the message bus
        * asyncio by default
        * multiprocessing when mp=True
        By default, there are no size limit enforced (maxsize=-1)
        """
        assert name not in self.queues, f"Queue {name} already setup"
        if self.redis_enabled and redis:
            self.queues[name] = RedisQueue(f"libmozevent:{name}")
        elif mp:
            self.queues[name] = multiprocessing.Queue(maxsize=maxsize)
        else:
            self.queues[name] = asyncio.Queue(maxsize=maxsize)

    async def send(self, name: str, payload: Any):
        """
        Send a message on a specific queue
        """
        assert name in self.queues, f"Missing queue {name}"
        queue = self.queues[name]

        if isinstance(queue, RedisQueue):
            redis = await AsyncRedis.connect()
            assert redis is not None
            await redis.rpush(
                queue.name, pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
            )

        elif isinstance(queue, asyncio.Queue):
            await queue.put(payload)

        else:
            # Run the synchronous mp queue.put in the asynchronous loop
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: queue.put(payload)
            )

    async def receive(self, name: str):
        """
        Wait for a message on a specific queue
        This is a blocking operation
        """
        assert name in self.queues, f"Missing queue {name}"
        queue = self.queues[name]

        logger.debug("Wait for message on bus", queue=name, instance=queue)

        if isinstance(queue, RedisQueue):

            # Clean the last processed message as we are awaiting a new one on this queue
            if name in self.redis_messages:
                del self.redis_messages[name]

            redis = await AsyncRedis.connect()
            assert redis is not None
            _, payload = await redis.blpop(queue.name)
            assert isinstance(payload, bytes)

            # Save the currently processed message
            # This ensures that we can restore the message in case of a failure
            self.redis_messages[name] = payload

            try:
                return pickle.loads(payload)
            except Exception as e:
                logger.error("Bad redis payload", error=str(e), exc_info=True)
                await asyncio.sleep(1)
                return

        elif isinstance(queue, asyncio.Queue):
            return await queue.get()

        else:
            # Run the synchronous mp queue.get in the asynchronous loop
            # but use an asyncio sleep to be able to react to cancellation
            async def _get():
                while True:
                    try:
                        return queue.get(timeout=0)
                    except Empty:
                        await asyncio.sleep(1)

            return await _get()

    async def restore_redis_messages(self):
        """
        Restores a currently processed message for each Redis queue on this bus.
        Parallel jobs on a same queue will be ignored, as the last processed
        message is erased when awaiting a new message on the queue.
        This method can be called when a failure occurs while running jobs on a
        bus queue, in order to restore non fully processed messages in Redis.
        """
        logger.info("Restoring non processed messages")
        redis = await AsyncRedis.connect()
        assert redis is not None
        for queue_name, payload in self.redis_messages.items():
            queue = self.queues[queue_name]
            # Insert message in the top of the Redis queue
            await redis.lpush(queue.name, payload)

    async def run(
        self,
        method: Callable,
        input_name: str,
        output_names: list = [],
        sequential: bool = True,
    ):
        """
        Pass messages from input to output
        Optionally applies some conversions methods
        This is also the "ideal" usage between 2 queues
        """
        assert input_name in self.queues, f"Missing queue {input_name}"
        for output_name in output_names:
            assert (
                output_name is None or output_name in self.queues
            ), f"Missing queue {output_name}"

        assert (
            sequential is True or len(output_names) == 0
        ), "Parallel run is not supported yet when an output queue is defined"

        while True:
            message = await self.receive(input_name)

            # Run async or sync methods
            if inspect.iscoroutinefunction(method):
                if sequential:
                    new_message = await method(message)
                else:
                    asyncio.create_task(method(message))
            else:
                assert sequential, "Can't run normal functions in parallel"
                new_message = method(message)

            for output_name in output_names:
                if new_message:
                    await self.send(output_name, new_message)
                else:
                    logger.info(
                        "Skipping new message creation: no result", message=message
                    )

    async def dispatch(self, input_name: str, output_names: list):
        await self.run(lambda m: m, input_name, output_names)
