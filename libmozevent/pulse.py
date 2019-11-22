# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio
import json

import aioamqp
import structlog

logger = structlog.get_logger(__name__)


async def create_pulse_listener(user, password, exchange, topic, callback):
    """
    Create an async consumer for Mozilla pulse queues
    Inspired by : https://github.com/mozilla-releng/fennec-aurora-task-creator/blob/master/fennec_aurora_task_creator/worker.py  # noqa
    """
    assert isinstance(user, str)
    assert isinstance(password, str)
    assert isinstance(exchange, str)
    assert isinstance(topic, str)

    host = "pulse.mozilla.org"
    port = 5671

    _, protocol = await aioamqp.connect(
        host=host, login=user, password=password, ssl=True, port=port
    )

    channel = await protocol.channel()
    await channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)

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

    logger.info("Connected on pulse", queue=queue, topic=topic, exchange=exchange)

    await channel.queue_bind(
        exchange_name=exchange, queue_name=queue, routing_key=topic
    )
    await channel.basic_consume(callback, queue_name=queue)

    return protocol


class PulseListener(object):
    """
    Pulse queues connector to receive external messages and react to them
    """

    def __init__(self, output_queue_name, queue, route, user, password):
        self.queue_name = output_queue_name
        self.queue = queue
        self.route = route
        self.user = user
        self.password = password

    def register(self, bus):
        self.bus = bus
        self.bus.add_queue(self.queue_name)

    async def connect(self):
        protocol = await create_pulse_listener(
            self.user, self.password, self.queue, self.route, self.got_message
        )
        logger.info("Worker starts consuming messages", queue=self.queue)
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

    async def got_message(self, channel, body, envelope, properties):
        """
        Generic Pulse consumer callback
        """
        assert isinstance(body, bytes), "Body is not in bytes"

        body = json.loads(body.decode("utf-8"))

        # Push the message in the message bus
        logger.debug("Received a pulse message")
        await self.bus.send(self.queue_name, body)

        # Ack the message so it is removed from the broker's queue
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
