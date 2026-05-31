"""RabbitMQ implementation of the ``DinnerEventPublisher`` port."""

from __future__ import annotations

import logging

import pika
from pika.exceptions import AMQPError

from restaurant_service.application.ports import DinnerEventPublisher, PublishError
from shared.messaging import topology
from shared.messaging.events import DinnerEvent
from shared.messaging.security import mask_card_number
from shared.messaging.serialization import serialize
from shared.messaging.topology import BrokerSettings, build_connection_parameters

logger = logging.getLogger(__name__)

_DELIVERY_MODE_PERSISTENT = 2


class RabbitMQDinnerPublisher(DinnerEventPublisher):
    """Publishes dinner events to the ``direct`` exchange (Exchange -> Queue).

    Declares the exchange, the durable queue and their binding so the topology
    matches Figure 1 regardless of which side connects first.
    """

    def __init__(self, settings: BrokerSettings) -> None:
        self._settings = settings

    def publish(self, event: DinnerEvent) -> None:
        try:
            connection = pika.BlockingConnection(
                build_connection_parameters(self._settings)
            )
            try:
                self._publish_on(connection, event)
            finally:
                connection.close()
        except AMQPError as exc:
            raise PublishError(f"could not publish dinner event: {exc}") from exc

    def _publish_on(
        self, connection: pika.BlockingConnection, event: DinnerEvent
    ) -> None:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=topology.EXCHANGE,
            exchange_type=topology.EXCHANGE_TYPE,
            durable=True,
        )
        channel.queue_declare(queue=topology.DINNER_QUEUE, durable=True)
        channel.queue_bind(
            queue=topology.DINNER_QUEUE,
            exchange=topology.EXCHANGE,
            routing_key=topology.DINNER_ROUTING_KEY,
        )
        channel.basic_publish(
            exchange=topology.EXCHANGE,
            routing_key=topology.DINNER_ROUTING_KEY,
            body=serialize(event),
            properties=pika.BasicProperties(
                delivery_mode=_DELIVERY_MODE_PERSISTENT,
                content_type="application/json",
            ),
        )
        logger.info(
            "Published dinner event card=%s restaurant=%s",
            mask_card_number(event.card_number),
            event.restaurant_code,
        )
