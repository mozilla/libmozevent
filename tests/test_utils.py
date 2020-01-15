# -*- coding: utf-8 -*-
import asyncio

from libmozevent import utils


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
