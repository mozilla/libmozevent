# -*- coding: utf-8 -*-
import asyncio
import multiprocessing
import os

import pytest

from libmozevent.bus import MessageBus, RedisQueue
from libmozevent.utils import AsyncRedis

QUEUE_TYPES = [{"mp": False, "redis": False}, {"mp": True, "redis": False}]
if "REDIS_URL" in os.environ:
    QUEUE_TYPES.append({"mp": False, "redis": True})


def test_queue_creation():
    """
    Test queues creation with different types
    """
    bus = MessageBus()
    assert len(bus.queues) == 0

    bus.add_queue("test")
    assert len(bus.queues) == 1

    with pytest.raises(AssertionError) as e:
        bus.add_queue("test")
    assert str(e.value) == "Queue test already setup"
    assert len(bus.queues) == 1

    bus.add_queue("another")
    assert len(bus.queues) == 2

    bus.add_queue("different", mp=True)
    assert len(bus.queues) == 3

    assert isinstance(bus.queues["test"], asyncio.Queue)
    assert isinstance(bus.queues["another"], asyncio.Queue)
    assert isinstance(bus.queues["different"], multiprocessing.queues.Queue)


@pytest.mark.parametrize("queue_type", QUEUE_TYPES)
@pytest.mark.asyncio
async def test_message_passing_async(queue_type):
    """
    Test sending & receiving messages on an async queue
    """

    async def do_test(redis_conn=None):
        bus = MessageBus(redis_conn)
        bus.add_queue("test", mp=queue_type["mp"], redis=queue_type["redis"])
        if not queue_type["mp"] and not queue_type["redis"]:
            assert isinstance(bus.queues["test"], asyncio.Queue)
        elif queue_type["mp"]:
            assert isinstance(bus.queues["test"], multiprocessing.queues.Queue)
        elif queue_type["redis"]:
            assert isinstance(bus.queues["test"], RedisQueue)

        await bus.send("test", {"payload": 1234})
        await bus.send("test", {"another": "deadbeef"})
        await bus.send("test", "covfefe")
        if not queue_type["redis"]:
            assert bus.queues["test"].qsize() == 3

        msg = await bus.receive("test")
        assert msg == {"payload": 1234}
        msg = await bus.receive("test")
        assert msg == {"another": "deadbeef"}
        msg = await bus.receive("test")
        assert msg == "covfefe"
        if not queue_type["redis"]:
            assert bus.queues["test"].qsize() == 0

    if queue_type["redis"]:
        async with AsyncRedis() as redis_conn:
            await do_test(redis_conn)
    else:
        await do_test()


@pytest.mark.parametrize("queue_type", QUEUE_TYPES)
@pytest.mark.asyncio
async def test_run_async_without_output_queue(queue_type):
    """
    Test using run to get messages from a queue using an async function and without an output queue
    """
    # Can't run with redis because it doesn't support maxsize.
    if queue_type["redis"]:
        return

    bus = MessageBus()
    bus.add_queue("input", mp=queue_type["mp"])
    if not queue_type["mp"]:
        assert isinstance(bus.queues["input"], asyncio.Queue)
    elif queue_type["mp"]:
        assert isinstance(bus.queues["input"], multiprocessing.queues.Queue)
    bus.add_queue("end", mp=queue_type["mp"], maxsize=1)
    assert bus.queues["input"].qsize() == 0

    await bus.send("input", "test x")
    await bus.send("input", "hello world.")

    assert bus.queues["input"].qsize() == 2

    count = 0

    async def torun(payload):
        nonlocal count

        if count == 0:
            assert payload == "test x"
        elif count == 1:
            assert payload == "hello world."
            await bus.send("end", count)
        else:
            assert False

        count += 1

    task = asyncio.create_task(bus.run(torun, "input"))

    await bus.receive("end") == 1
    task.cancel()
    assert bus.queues["input"].qsize() == 0
    assert bus.queues["end"].qsize() == 0


@pytest.mark.parametrize("queue_type", QUEUE_TYPES)
@pytest.mark.asyncio
async def test_run_sync_without_output_queue(queue_type):
    """
    Test using run to get messages from a queue using a function and without an output queue
    """
    # Can't run with redis because it doesn't support maxsize.
    if queue_type["redis"]:
        return

    bus = MessageBus()
    bus.add_queue("input", mp=queue_type["mp"])
    if not queue_type["mp"]:
        assert isinstance(bus.queues["input"], asyncio.Queue)
    elif queue_type["mp"]:
        assert isinstance(bus.queues["input"], multiprocessing.queues.Queue)
    bus.add_queue("end", mp=queue_type["mp"], maxsize=1)
    assert bus.queues["input"].qsize() == 0

    await bus.send("input", "test x")
    await bus.send("input", "hello world.")

    assert bus.queues["input"].qsize() == 2

    count = 0

    def torun(payload):
        nonlocal count

        if count == 0:
            assert payload == "test x"
        elif count == 1:
            assert payload == "hello world."
            asyncio.create_task(bus.send("end", count))
        else:
            assert False

        count += 1

    task = asyncio.create_task(bus.run(torun, "input"))

    await bus.receive("end") == 1
    task.cancel()
    assert bus.queues["input"].qsize() == 0
    assert bus.queues["end"].qsize() == 0


@pytest.mark.parametrize("queue_type", QUEUE_TYPES)
@pytest.mark.asyncio
async def test_conversion(queue_type):
    """
    Test message conversion between 2 queues
    """
    # Can't run with redis because it doesn't support maxsize.
    if queue_type["redis"]:
        return

    bus = MessageBus()
    bus.add_queue("input", mp=queue_type["mp"])
    if not queue_type["mp"]:
        assert isinstance(bus.queues["input"], asyncio.Queue)
    elif queue_type["mp"]:
        assert isinstance(bus.queues["input"], multiprocessing.queues.Queue)
    bus.add_queue(
        "output", mp=queue_type["mp"], maxsize=3
    )  # limit size to immediately stop execution for unit test
    if not queue_type["mp"]:
        assert isinstance(bus.queues["output"], asyncio.Queue)
    elif queue_type["mp"]:
        assert isinstance(bus.queues["output"], multiprocessing.queues.Queue)
    assert bus.queues["input"].qsize() == 0
    assert bus.queues["output"].qsize() == 0

    await bus.send("input", "test x")
    await bus.send("input", "hello world.")
    await bus.send("output", "lowercase")

    # Convert all strings from input in uppercase
    assert bus.queues["input"].qsize() == 2
    task = asyncio.create_task(bus.run(lambda x: x.upper(), "input", ["output"]))

    await bus.receive("output") == "lowercase"
    await bus.receive("output") == "TEST X"
    await bus.receive("output") == "HELLO WORLD."
    task.cancel()
    assert bus.queues["input"].qsize() == 0
    assert bus.queues["output"].qsize() == 0


@pytest.mark.parametrize("queue_type", QUEUE_TYPES)
@pytest.mark.asyncio
async def test_run_async_parallel(queue_type):
    """
    Test using run to get messages from a queue using an async function executed in parallel
    """
    # Can't run with redis because it doesn't support maxsize.
    if queue_type["redis"]:
        return

    bus = MessageBus()
    bus.add_queue("input", mp=queue_type["mp"])
    if not queue_type["mp"]:
        assert isinstance(bus.queues["input"], asyncio.Queue)
    elif queue_type["mp"]:
        assert isinstance(bus.queues["input"], multiprocessing.queues.Queue)
    bus.add_queue("end", mp=queue_type["mp"], maxsize=1)
    assert bus.queues["input"].qsize() == 0

    await bus.send("input", 0)
    await bus.send("input", 1)
    await bus.send("input", 2)

    assert bus.queues["input"].qsize() == 3

    done = {
        0: False,
        1: False,
        2: False,
    }

    async def torun(count):
        await asyncio.sleep(0)

        if count == 0:
            # Wait for 7 seconds, in the meantime other tasks will be scheduled
            # and executed.
            await asyncio.sleep(7)
        elif count == 1:
            pass
        elif count == 2:
            await bus.send("end", count)
        else:
            assert False

        done[count] = True

    task = asyncio.create_task(bus.run(torun, "input", sequential=False))

    await bus.receive("end") == 1
    task.cancel()
    assert bus.queues["input"].qsize() == 0
    assert bus.queues["end"].qsize() == 0
    assert done == {
        0: False,
        1: True,
        2: True,
    }


@pytest.mark.parametrize("queue_type", QUEUE_TYPES)
@pytest.mark.asyncio
async def test_dispatch(queue_type):
    """
    Test message dispatch from a queue to 2 queues
    """
    # Can't run with redis because it doesn't support maxsize.
    if queue_type["redis"]:
        return

    bus = MessageBus()
    bus.add_queue("input", mp=queue_type["mp"])
    # limit size to immediately stop execution for unit test
    bus.add_queue("output1", mp=queue_type["mp"], maxsize=3)
    bus.add_queue("output2", mp=queue_type["mp"], maxsize=3)
    if not queue_type["mp"]:
        assert isinstance(bus.queues["input"], asyncio.Queue)
    elif queue_type["mp"]:
        assert isinstance(bus.queues["input"], multiprocessing.queues.Queue)
    if not queue_type["mp"]:
        assert isinstance(bus.queues["output1"], asyncio.Queue)
    elif queue_type["mp"]:
        assert isinstance(bus.queues["output1"], multiprocessing.queues.Queue)
    if not queue_type["mp"]:
        assert isinstance(bus.queues["output2"], asyncio.Queue)
    elif queue_type["mp"]:
        assert isinstance(bus.queues["output2"], multiprocessing.queues.Queue)
    assert bus.queues["input"].qsize() == 0
    assert bus.queues["output1"].qsize() == 0
    assert bus.queues["output2"].qsize() == 0

    await bus.send("input", 1)
    await bus.send("input", 2)

    # Convert all strings from input in uppercase
    assert bus.queues["input"].qsize() == 2
    task = asyncio.create_task(bus.dispatch("input", ["output1", "output2"]))

    await bus.receive("output1") == 1
    await bus.receive("output2") == 1
    await bus.receive("output1") == 2
    await bus.receive("output2") == 2
    task.cancel()
    assert bus.queues["input"].qsize() == 0
    assert bus.queues["output1"].qsize() == 0
    assert bus.queues["output2"].qsize() == 0


@pytest.mark.parametrize("queue_type", QUEUE_TYPES)
@pytest.mark.asyncio
async def test_maxsize(queue_type):
    """
    Test a queue maxsize behaves as expected
    Maxsize=-1 is enabled by default
    """
    # Can't run with redis because it doesn't support maxsize.
    if queue_type["redis"]:
        return

    bus = MessageBus()
    bus.add_queue("test", mp=queue_type["mp"])
    # No maxsize getter on mp queues
    if not queue_type["mp"]:
        assert bus.queues["test"].maxsize == -1

    assert bus.queues["test"].empty()

    for i in range(1000):
        await bus.send("test", i)

    assert not bus.queues["test"].full()
