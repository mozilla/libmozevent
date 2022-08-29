# -*- coding: utf-8 -*-
import asyncio
import os
import pickle
import signal
from unittest.mock import Mock

import pytest

from libmozevent import utils
from libmozevent.bus import AsyncRedis
from libmozevent.bus import MessageBus
from libmozevent.bus import RedisQueue


def unexpected_error():
    raise Exception("Unexpected")


def test_run_tasks():
    count = 0

    async def do():
        nonlocal count
        count += 1
        await asyncio.sleep(0)

    utils.run_tasks([do(), do(), do()])
    assert count == 3

    utils.run_tasks(f for f in [do(), do(), do()])
    assert count == 6

    utils.run_tasks(set([do(), do(), do()]))
    assert count == 9


@pytest.mark.parametrize(
    "task_side_effect, expected_exception",
    [
        (lambda: os.kill(os.getpid(), signal.SIGTERM), asyncio.CancelledError),
        (unexpected_error, Exception),
    ],
)
def test_run_tasks_restore_redis_messages(task_side_effect, expected_exception):
    """
    When using a Redis queue, messages are restored in case of an unexpected failure or
    if a SIGTERM is received
    """
    redis_mock = Mock(spec=RedisQueue)

    async def redis_connect():
        return redis_mock

    AsyncRedis.connect = Mock(side_effect=redis_connect)

    redis_queue = []

    async def await_rpush(*args):
        redis_queue.append(args)

    async def await_lpush(*args):
        redis_queue.insert(0, args)

    async def await_blpop(*args):
        return redis_queue.pop(0)

    redis_mock.rpush = await_rpush
    redis_mock.lpush = await_lpush
    redis_mock.blpop = await_blpop

    os.environ.update(REDIS_URL="redis://localhost")
    bus = MessageBus()

    bus.add_queue("input", redis=True)
    assert len(bus.queues) == 1
    assert redis_queue == []

    asyncio.get_event_loop().run_until_complete(bus.send("input", "message"))
    queue_name = bus.queues["input"].name
    message = pickle.dumps("message", protocol=pickle.HIGHEST_PROTOCOL)

    assert redis_queue == [(queue_name, message)]

    async def deadly_task():
        await bus.receive("input")
        # Ensure the message has been consumed
        assert redis_queue == []
        # Raise an unexpected error or send a TERM signal to itself
        task_side_effect()
        await asyncio.sleep(10)

    with pytest.raises(expected_exception):
        utils.run_tasks([deadly_task()], bus_to_restore=bus)

    if expected_exception == asyncio.CancelledError:
        # In case of a SIGTERM, the message has been restored in the queue
        assert redis_queue == [(queue_name, message)]
    else:
        assert redis_queue == []
