# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio
import fnmatch
import itertools
import json
from typing import Dict, List, Tuple

import aioamqp
import structlog

logger = structlog.get_logger(__name__)

BusQueue = str
Queue = str
Route = str
PulseBinding = Tuple[Queue, List[Route]]


async def create_pulse_listener(
    user, password, exchanges_topics: List[PulseBinding], callback, virtualhost="/"
):
    """
    Create an async consumer for Mozilla pulse queues
    Inspired by : https://github.com/mozilla-releng/fennec-aurora-task-creator/blob/master/fennec_aurora_task_creator/worker.py  # noqa
    """
    assert isinstance(user, str)
    assert isinstance(password, str)

    host = "pulse.mozilla.org"
    port = 5671

    _, protocol = await aioamqp.connect(
        host=host,
        login=user,
        password=password,
        ssl=True,
        port=port,
        virtualhost=virtualhost,
    )

    channel = await protocol.channel()
    await channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)

    for exchange, topics in exchanges_topics:
        # get exchange name out from full exchange name
        exchange_name = exchange
        if exchange.startswith(f"exchange/{user}/"):
            exchange_name = exchange[len(f"exchange/{user}/") :]
        elif exchange.startswith(f"exchange/"):
            exchange_name = exchange[len(f"exchange/") :]

        # full exchange name should start with "exchange/"
        if not exchange.startswith("exchange/"):
            exchange = f"exchange/{exchange}"

        # queue is required to:
        # - start with "queue/"
        # - user should follow the "queue/"
        # - after that "exchange/" should follow, this is not requirement from
        #   pulse but something we started doing in release services
        queue = f"queue/{user}/exchange/{exchange_name}"

        await channel.queue_declare(queue_name=queue, durable=True)

        # in case we are going to listen to an exchange that is specific for this
        # user, we need to ensure that exchange exists before first message is
        # sent (this is what creates exchange)
        if exchange.startswith(f"exchange/{user}/"):
            await channel.exchange_declare(
                exchange_name=exchange, type_name="topic", durable=True
            )

        for topic in topics:
            logger.info(
                "Connected on pulse", queue=queue, topic=topic, exchange=exchange
            )

            await channel.queue_bind(
                exchange_name=exchange, queue_name=queue, routing_key=topic
            )

        await channel.basic_consume(callback, queue_name=queue)

    return protocol


class PulseListener(object):
    """
    Pulse queues connector to receive external messages and react to them
    """

    def __init__(
        self,
        queues_routes: Dict[BusQueue, List[PulseBinding]],
        user,
        password,
        virtualhost="/",
    ):
        self.queues_routes = queues_routes
        self.user = user
        self.password = password
        self.virtualhost = virtualhost

    def register(self, bus):
        self.bus = bus
        for name in self.queues_routes:
            self.bus.add_queue(name)

    async def connect(self):
        protocol = await create_pulse_listener(
            self.user,
            self.password,
            itertools.chain(*self.queues_routes.values()),
            self.got_message,
            self.virtualhost,
        )
        logger.info(
            "Worker starts consuming messages", queues_routes=self.queues_routes
        )
        return protocol

    async def run(self):
        pulse = None
        while True:
            try:
                if pulse is None:
                    pulse = await self.connect()

                # Check pulse server is still connected
                # AmqpClosedConnection will be thrown otherwise
                await pulse.ensure_open()
                await asyncio.sleep(7)
            except (aioamqp.AmqpClosedConnection, OSError) as e:
                logger.exception("Reconnecting pulse client in 5 seconds", error=str(e))
                pulse = None
                await asyncio.sleep(5)

    def find_matching_queues(self, payload_exchange: str, payload_routes: List[bytes]):
        """
        Detect all the bus that match the current routing
        """

        def _match(exchange, route):

            # Exchanges must match exactly
            if payload_exchange != exchange:
                return False

            # One of the pauload routes must match the current route
            route = route.replace("#", "*").encode("utf-8")
            return len(fnmatch.filter(payload_routes, route)) > 0

        return {
            bus_queue
            for bus_queue, queue_routes in self.queues_routes.items()
            for pulse_exchange, pulse_routes in queue_routes
            for pulse_route in pulse_routes
            if _match(pulse_exchange, pulse_route)
        }

    async def got_message(self, channel, body, envelope, properties):
        """
        Generic Pulse consumer callback that detects all matching bus queues
        to automatically route the pulse messages
        """
        assert isinstance(body, bytes), "Body is not in bytes"

        # Build routing information to identify the payload source
        routing = {
            "exchange": envelope.exchange_name,
            "key": envelope.routing_key,
            "other_routes": properties.headers.get("CC", []),
        }

        # Automatically decode json payloads
        if properties.content_type == "application/json":
            body = json.loads(body)

        # Push the message in the message bus
        logger.debug("Received a pulse message")
        routes = [routing["key"].encode("utf-8")] + routing["other_routes"]
        for bus_queue in self.find_matching_queues(routing["exchange"], routes):
            await self.bus.send(bus_queue, {"routing": routing, "body": body})

        # Ack the message so it is removed from the broker's queue
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
