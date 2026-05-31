"""AMQP topology and broker connection settings.

Names are unique per team because the course RabbitMQ server is shared by all
students. The host and credentials are read from environment variables, never
hardcoded (Security quality gate, RNF-04).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import pika

# --- AMQP topology (Exchange -> Routing key -> Queue), per Figure 1 ----------
EXCHANGE = "recompensas_exchange_Sebastian_Hernandez_t1"
EXCHANGE_TYPE = "direct"

DINNER_QUEUE = "cenas_registradas_Sebastian_Hernandez_t1"
DINNER_ROUTING_KEY = "cena.registrada"

NOTIFICATION_QUEUE = "notificaciones_Sebastian_Hernandez_t1"
NOTIFICATION_ROUTING_KEY = "recompensa.procesada"


@dataclass(frozen=True)
class BrokerSettings:
    """RabbitMQ connection parameters loaded from the environment."""

    host: str
    port: int
    user: str
    password: str
    virtual_host: str

    @classmethod
    def from_env(cls) -> "BrokerSettings":
        return cls(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            user=os.getenv("RABBITMQ_USER", "students"),
            password=os.getenv("RABBITMQ_PASSWORD", ""),
            virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
        )


def build_connection_parameters(settings: BrokerSettings) -> pika.ConnectionParameters:
    """Build pika connection parameters from broker settings.

    Centralised here to avoid duplicating credentials wiring in every adapter.
    """
    credentials = pika.PlainCredentials(settings.user, settings.password)
    return pika.ConnectionParameters(
        host=settings.host,
        port=settings.port,
        virtual_host=settings.virtual_host,
        credentials=credentials,
    )
