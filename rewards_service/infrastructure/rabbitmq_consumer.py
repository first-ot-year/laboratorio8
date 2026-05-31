"""RabbitMQ consumer of the dinner queue (drives CU-02)."""

from __future__ import annotations

import logging

import pika

from rewards_service.application.ports import PublishError, RepositoryError
from rewards_service.application.process_dinner import ProcessDinner
from shared.messaging import topology
from shared.messaging.events import DinnerEvent
from shared.messaging.serialization import SerializationError, deserialize
from shared.messaging.topology import BrokerSettings, build_connection_parameters

logger = logging.getLogger(__name__)


class DinnerConsumer:
    """Consumes dinner events and delegates processing to the use case.

    Message handling is split from the connection lifecycle so the acknowledge
    logic can be unit tested without a live broker.
    """

    def __init__(self, settings: BrokerSettings, process_dinner: ProcessDinner) -> None:
        self._settings = settings
        self._process_dinner = process_dinner

    def on_message(self, channel, method, _properties, body: bytes) -> None:
        """Handle one delivery: deserialize, process and acknowledge."""
        try:
            event = deserialize(body, DinnerEvent)
        except SerializationError:
            logger.warning("Discarding malformed dinner message")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            self._process_dinner.execute(event)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except (RepositoryError, PublishError):
            logger.exception("Processing failed; requeuing message")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start(self) -> None:  # pragma: no cover - requires a live broker
        connection = pika.BlockingConnection(
            build_connection_parameters(self._settings)
        )
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
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=topology.DINNER_QUEUE, on_message_callback=self.on_message
        )
        logger.info("Waiting for dinner events. Press CTRL+C to exit.")
        channel.start_consuming()
