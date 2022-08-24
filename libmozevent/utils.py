# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import contextvars
import fcntl
import os
import time
from typing import Iterable

import aioredis
import hglib
import structlog

log = structlog.get_logger(__name__)


def run_tasks(awaitables: Iterable):
    """
    Helper to run tasks concurrently, but when an exception is raised
    by one of the tasks, the whole stack stops.
    """

    async def _run():
        try:
            # Create a task grouping all awaitables
            # and running them concurrently
            task = asyncio.gather(*awaitables)
            await task
        except Exception as e:
            log.error("Failure while running async tasks", error=str(e))

            # When ANY exception from one of the awaitables
            # make sure the other awaitables are cancelled
            task.cancel()

    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(_run())


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
        log.error("Mercurial {} failure".format(main_cmd), out=out, err=err)
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


class WorkerMixin(object):
    """
    A worker listening to a bus event that can be stopped anytime
    The stop method is not completely safe against concurrent accesses
    """

    _is_running = True
    # Mutex to ensure the worker does not start a task while being stopped
    _lock = asyncio.Lock()
    # Store the task awaiting a message
    _message_await = None
    # And the task consuming the message.
    # Current task is either None or a tuple containing the task and the message
    _current_task = None

    async def stop(self):
        """
        Stop the current tasks then the worker itself
        Returns None if no task were actually running
        Returns the initial message in case the task was not finished
        """
        # Use a mutex lock to prevent a new task to be started
        async with self._lock:
            self._is_running = False

            # Cancel message awaiting async job
            if self._message_await is not None:
                self.message_await.cancel()
                self.message_await = None

            # Cancel a potential running task
            if self._current_task is None:
                return None

            task, message = self._current_task
            task.cancel()
            self._current_task = None
            return message

    async def process_message(self, payload, *args, **kwargs):
        raise NotImplementedError

    async def get_message(self, *args, **kwargs):
        # In the default implementation, use worker attributes
        assert getattr(self, "queue_name", None) and getattr(
            self, "bus", None
        ), "A queue name and a bus must be defined to run the worker"
        return await self.bus.receive(self.queue_name)

    async def run(self, *args, **kwargs):
        while self._is_running:
            if self._lock.locked():
                # We are currently stopping the worker
                continue

            try:
                # Wait for a new message in the queue
                self._message_await = asyncio.create_task(
                    self.get_message(*args, **kwargs)
                )
                payload = await self._message_await
                self._message_await = None

                # Process message
                task = asyncio.create_task(
                    self.process_message(payload, *args, **kwargs)
                )
                self._current_task = (task, payload)
                await task
                self._current_task = None

            except asyncio.CancelledError:
                log.warning("Stopped any async tasks handled by the worker")


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
