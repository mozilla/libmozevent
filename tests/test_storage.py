# -*- coding: utf-8 -*-
import asyncio
import os

import pytest

from libmozevent.storage import EphemeralStorage
from libmozevent.utils import AsyncRedis

REDIS_PARAMS = [False]
if "REDIS_URL" in os.environ:
    REDIS_PARAMS.append(True)


@pytest.mark.parametrize("redis_param", REDIS_PARAMS)
@pytest.mark.asyncio
async def test_ephemeral_storage(redis_param):
    obj1 = {"an": "object"}
    obj2 = {"another": "object"}
    obj3 = {"yet": {"another": "object"}}

    async def do_test(redis_conn=None):
        storage = EphemeralStorage("my_first_set", 60, redis_conn)

        await storage.set("my_key1", obj1)
        await storage.set("my_key2", obj2)
        await storage.set("my_key3", obj3)

        got_obj1 = await storage.get("my_key1")
        assert got_obj1 == obj1

        got_obj1 = await storage.get("my_key1")
        assert got_obj1 == obj1

        got_obj2 = await storage.get("my_key2")
        assert got_obj2 == obj2

        with pytest.raises(KeyError):
            await storage.get("my_key9")

        await storage.rem("my_key2")

        with pytest.raises(KeyError):
            await storage.get("my_key2")

        got_obj1 = await storage.get("my_key1")
        assert got_obj1 == obj1

        got_obj3 = await storage.get("my_key3")
        assert got_obj3 == obj3

    if redis_param:
        async with AsyncRedis() as redis_conn:
            await do_test(redis_conn)
    else:
        await do_test()


@pytest.mark.parametrize("redis_param", REDIS_PARAMS)
@pytest.mark.asyncio
async def test_ephemeral_storage_expiration(redis_param):
    """
    Test a key which expires.
    """

    async def do_test(redis_conn=None):
        obj = {"an": "object"}

        storage = EphemeralStorage("my_second_set", 1, redis_conn)

        await storage.set("my_key", obj)

        got_obj = await storage.get("my_key")
        assert got_obj == obj

        await asyncio.sleep(1)

        storage = EphemeralStorage("my_second_set", 1, redis_conn)

        with pytest.raises(KeyError):
            await storage.get("my_key")

    if redis_param:
        async with AsyncRedis() as redis_conn:
            await do_test(redis_conn)
    else:
        await do_test()


@pytest.mark.asyncio
async def test_ephemeral_storage_no_expiration():
    """
    Test a key which does not expire.
    """
    # This test only works when Redis is available.
    if "REDIS_URL" not in os.environ:
        return

    async with AsyncRedis() as redis_conn:
        obj = {"an": "object"}

        storage = EphemeralStorage("my_third_set", 60, redis_conn)

        await storage.set("my_key", obj)

        got_obj = await storage.get("my_key")
        assert got_obj == obj

        await asyncio.sleep(1)

        storage = EphemeralStorage("my_third_set", 60, redis_conn)

        assert await storage.get("my_key") == obj


@pytest.mark.parametrize("redis_param", REDIS_PARAMS)
@pytest.mark.asyncio
async def test_ephemeral_storage_no_expiration_remove(redis_param):
    """
    Test deleting a key after the recreation of a Storage.
    """

    async def do_test(redis_conn=None):
        obj = {"an": "object"}

        storage = EphemeralStorage("my_third_set", 60, redis_conn)

        await storage.set("my_key", obj)

        got_obj = await storage.get("my_key")
        assert got_obj == obj

        await asyncio.sleep(1)

        storage = EphemeralStorage("my_third_set", 60, redis_conn)

        await storage.rem("my_key")

    if redis_param:
        async with AsyncRedis() as redis_conn:
            await do_test(redis_conn)
    else:
        await do_test()
