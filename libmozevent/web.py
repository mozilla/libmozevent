# -*- coding: utf-8 -*-
import os
from multiprocessing import Process

import structlog
from aiohttp import web

from libmozevent.phabricator import PhabricatorBuild

logger = structlog.get_logger(__name__)


class WebServer(object):
    """
    WebServer used to receive hook
    """

    def __init__(self, queue_name, version_path="/app/version.json"):
        self.process = None
        self.http_port = int(os.environ.get("PORT", 9000))
        self.queue_name = queue_name
        logger.info("HTTP webhook server will listen", port=self.http_port)

        self.version_path = version_path

        # Configure the web application with code review routes
        self.app = web.Application()
        self.app.add_routes(
            [
                web.get("/__version__", self.get_version),
                web.get("/__heartbeat__", self.get_heartbeat),
                web.get("/__lbheartbeat__", self.get_lbheartbeat),
                web.get("/ping", self.ping),
                web.post("/codereview/new", self.create_code_review),
            ]
        )

    def register(self, bus):
        self.bus = bus
        self.bus.add_queue(self.queue_name, mp=True, redis=True)

    def start(self):
        """
        Run the web server used by hooks in its own process
        """

        def _run():
            web.run_app(self.app, port=self.http_port, print=logger.info)

        # Run webserver in its own process
        self.process = Process(target=_run)
        self.process.start()
        logger.info("Web server started", pid=self.process.pid)

        return self.process

    def stop(self):
        assert self.process is not None, "Web server not started"
        self.process.kill()
        logger.info("Web server stopped")

    async def get_version(self, request):
        """
        HTTP GET version
        Following Dockerflow protocol
        """
        try:
            with open(self.version_path, "r") as version_file:
                version = version_file.read()
        except Exception:
            return web.HTTPInternalServerError(
                reason="Could not retrieve the version file"
            )

        return web.json_response(version)

    async def get_heartbeat(self, request):
        """
        HTTP GET heartbeat for backing services
        Following Dockerflow protocol
        """
        return web.HTTPOk()

    async def get_lbheartbeat(self, request):
        """
        HTTP GET heartbeat for load balancer
        Following Dockerflow protocol
        """
        return web.HTTPOk()

    async def ping(self, request):
        """
        Dummy test endpoint
        """
        return web.Response(text="pong")

    async def create_code_review(self, request):
        """
        HTTP POST webhook used by HarborMaster on new builds
        It only stores build ids and reply ASAP
        Mandatory query parameters:
        * diff as ID
        * repo as PHID
        * revision as ID
        * target as PHID
        """
        try:
            build = PhabricatorBuild(request)
            await self.bus.send(self.queue_name, build)
        except Exception as e:
            logger.error(str(e), path=request.path_qs, exc_info=True)
            raise web.HTTPBadRequest(text=str(e))

        logger.info("Queued new build", build=str(build))
        return web.Response(text="Build queued")
