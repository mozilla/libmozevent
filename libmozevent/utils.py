# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import contextvars
import fcntl
import os
import signal
import time
from typing import Iterable

import aioredis
import hglib
import structlog

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


def hg_run(cmd):
    """
    Run a mercurial command without an hglib instance
    Useful for initial custom clones
    Redirects stdout & stderr to python's logger
    """

    def _log_process(output, name):
        # Read and display every line
        out = output.read()
        if out is None:
            return
        text = filter(None, out.decode("utf-8").splitlines())
        for line in text:
            log.info("{}: {}".format(name, line))

    # Start process
    main_cmd = cmd[0]
    proc = hglib.util.popen([hglib.HGPATH] + cmd)

    # Set process outputs as non blocking
    for output in (proc.stdout, proc.stderr):
        fcntl.fcntl(
            output.fileno(),
            fcntl.F_SETFL,
            fcntl.fcntl(output, fcntl.F_GETFL) | os.O_NONBLOCK,
        )

    while proc.poll() is None:
        _log_process(proc.stdout, main_cmd)
        _log_process(proc.stderr, "{} (err)".format(main_cmd))
        time.sleep(2)

    out, err = proc.communicate()
    if proc.returncode != 0:
        log.error(
            "Mercurial {} failure".format(main_cmd), out=out, err=err, exc_info=True
        )
        raise hglib.error.CommandError(cmd, proc.returncode, out, err)

    return out


def batch_checkout(repo_url, repo_dir, revision=b"tip", batch_size=100000):
    """
    Helper to clone a mercurial repository using several steps
    to minimize memory footprint and stay below 1Gb of RAM
    It's used on Heroku small dynos, and support restarts
    """
    assert isinstance(revision, bytes)
    assert isinstance(batch_size, int)
    assert batch_size > 1

    log.info("Batch checkout", url=repo_url, dir=repo_dir, size=batch_size)
    try:
        cmd = hglib.util.cmdbuilder(
            "clone", repo_url, repo_dir, noupdate=True, verbose=True, stream=True
        )
        hg_run(cmd)
        log.info("Initial clone finished")
    except hglib.error.CommandError as e:
        if e.err.startswith(
            "abort: destination '{}' is not empty".format(repo_dir).encode("utf-8")
        ):
            log.info("Repository already present, skipping clone")
        else:
            raise

    repo = hglib.open(repo_dir)
    start = max(int(repo.identify(num=True).strip().decode("utf-8")), 1)
    target = int(repo.identify(rev=revision, num=True).strip().decode("utf-8"))
    if start >= target:
        return
    log.info("Will process checkout in range", start=start, target=target)

    steps = list(range(start, target, batch_size)) + [target]
    for rev in steps:
        log.info("Moving repo to revision", dir=repo_dir, rev=rev)
        repo.update(rev=rev)


def robust_checkout(repo_url, repo_dir, branch=b"tip"):
    """
    Helper to clone a mercurial repo using the robustcheckout extension
    """
    assert isinstance(branch, bytes)

    cmd = hglib.util.cmdbuilder(
        "robustcheckout",
        repo_url,
        repo_dir,
        purge=True,
        sharebase=f"{repo_dir}-shared",
        branch=branch,
    )
    hg_run(cmd)


class AsyncRedis(object):
    """
    Async context manager to create a redis connection
    """

    redis: aioredis.Redis = contextvars.ContextVar("redis-server")

    @staticmethod
    async def connect():
        if AsyncRedis.redis.get(None) is None:
            AsyncRedis.redis.set(
                await aioredis.from_url(
                    os.environ["REDIS_TLS_URL"],
                    decode_responses=False,
                    ssl_cert_reqs=None,
                )
                if os.getenv("REDIS_TLS_URL", None) is not None
                else await aioredis.from_url(
                    os.environ["REDIS_URL"],
                    decode_responses=False,
                )
            )
        return AsyncRedis.redis.get()
