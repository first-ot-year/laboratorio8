"""RabbitMQ implementation of ``NotificationPublisher``."""

from __future__ import annotations

import logging

import pika
from pika.exceptions import AMQPError

from rewards_service.application.ports import NotificationPublisher, PublishError
from shared.messaging import topology
from shared.messaging.events import RewardProcessedEvent
from shared.messaging.security import mask_card_number
from shared.messaging.serialization import serialize
from shared.messaging.topology import BrokerSettings, build_connection_parameters

logger = logging.getLogger(__name__)

_DELIVERY_MODE_PERSISTENT = 2


class RabbitMQNotificationPublisher(NotificationPublisher):
    """Publishes ``recompensa.procesada`` events to the notification queue."""

    def __init__(self, settings: BrokerSettings) -> None:
        self._settings = settings

    def publish(self, event: RewardProcessedEvent) -> None:
        try:
            connection = pika.BlockingConnection(
                build_connection_parameters(self._settings)
            )
            try:
                self._publish_on(connection, event)
            finally:
                connection.close()
        except AMQPError as exc:
            raise PublishError(f"could not publish notification: {exc}") from exc

    def _publish_on(
        self, connection: pika.BlockingConnection, event: RewardProcessedEvent
    ) -> None:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=topology.EXCHANGE,
            exchange_type=topology.EXCHANGE_TYPE,
            durable=True,
        )
        channel.queue_declare(queue=topology.NOTIFICATION_QUEUE, durable=True)
        channel.queue_bind(
            queue=topology.NOTIFICATION_QUEUE,
            exchange=topology.EXCHANGE,
            routing_key=topology.NOTIFICATION_ROUTING_KEY,
        )
        channel.basic_publish(
            exchange=topology.EXCHANGE,
            routing_key=topology.NOTIFICATION_ROUTING_KEY,
            body=serialize(event),
            properties=pika.BasicProperties(
                delivery_mode=_DELIVERY_MODE_PERSISTENT,
                content_type="application/json",
            ),
        )
        logger.info(
            "Published reward notification card=%s points=%s",
            mask_card_number(event.card_number),
            event.points,
        )
