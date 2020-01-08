# -*- coding: utf-8 -*-
import asyncio
import os

import pytest

from libmozevent.storage import EphemeralStorage


@pytest.mark.asyncio
async def test_ephemeral_storage():
    obj1 = {"an": "object"}
    obj2 = {"another": "object"}
    obj3 = {"yet": {"another": "object"}}

    storage = EphemeralStorage("my_first_set", 60)

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


@pytest.mark.asyncio
async def test_ephemeral_storage_expiration():
    """
    Test a key which expires.
    """
    obj = {"an": "object"}

    storage = EphemeralStorage("my_second_set", 1)

    await storage.set("my_key", obj)

    got_obj = await storage.get("my_key")
    assert got_obj == obj

    await asyncio.sleep(1)

    storage = EphemeralStorage("my_second_set", 1)

    with pytest.raises(KeyError):
        await storage.get("my_key")


@pytest.mark.asyncio
async def test_ephemeral_storage_no_expiration():
    """
    Test a key which does not expire.
    """
    # This test only works when Redis is available.
    if "REDIS_URL" not in os.environ:
        return

    obj = {"an": "object"}

    storage = EphemeralStorage("my_third_set", 60)

    await storage.set("my_key", obj)

    got_obj = await storage.get("my_key")
    assert got_obj == obj

    await asyncio.sleep(1)

    storage = EphemeralStorage("my_third_set", 60)

    assert await storage.get("my_key") == obj
