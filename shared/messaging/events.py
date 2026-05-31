"""Event contracts exchanged through the broker.

These dataclasses are pure data carriers (the messaging contract). They hold no
business logic so the producer and consumer domains stay decoupled: each service
owns its own rules and only shares the shape of the messages.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DinnerEvent:
    """A dinner registered by an affiliated restaurant (``cena.registrada``)."""

    transaction_id: str
    amount: float
    card_number: str
    restaurant_code: str
    timestamp: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "amount": self.amount,
            "card_number": self.card_number,
            "restaurant_code": self.restaurant_code,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DinnerEvent":
        return cls(
            transaction_id=data["transaction_id"],
            amount=data["amount"],
            card_number=data["card_number"],
            restaurant_code=data["restaurant_code"],
            timestamp=data["timestamp"],
        )


@dataclass(frozen=True)
class RewardProcessedEvent:
    """A reward credited to a customer account (``recompensa.procesada``)."""

    transaction_id: str
    card_number: str
    points: int
    cashback: float
    restaurant_code: str
    timestamp: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "card_number": self.card_number,
            "points": self.points,
            "cashback": self.cashback,
            "restaurant_code": self.restaurant_code,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RewardProcessedEvent":
        return cls(
            transaction_id=data["transaction_id"],
            card_number=data["card_number"],
            points=data["points"],
            cashback=data["cashback"],
            restaurant_code=data["restaurant_code"],
            timestamp=data["timestamp"],
        )
