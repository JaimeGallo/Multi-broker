"""Error hierarchy. Broker adapters translate native errors into these types."""

from __future__ import annotations


class JEVError(Exception):
    """Base class for every error raised by the platform."""


class ConfigError(JEVError):
    """Invalid or unsafe configuration."""


class SafetyError(JEVError):
    """The platform refuses to operate because a safety invariant would be violated."""


class LiveTradingNotAllowed(SafetyError):
    """Live trading was requested but is not allowed."""


class DataError(JEVError):
    """Market data problem (unsupported subscription, malformed payload...)."""


class ModelError(JEVError):
    """The predictive model failed to produce a valid prediction."""


class BrokerError(JEVError):
    """Base class for broker adapter errors."""


class BrokerUnavailable(BrokerError):
    """The request did not reach the broker (disconnected, not configured)."""


class OrderRejected(BrokerError):
    """The broker synchronously rejected the order."""

    def __init__(self, reason: str, client_order_id: str | None = None) -> None:
        super().__init__(f"order rejected: {reason}")
        self.reason = reason
        self.client_order_id = client_order_id


class DuplicateClientOrderId(BrokerError):
    """The broker already has an order with this client order id."""

    def __init__(self, client_order_id: str) -> None:
        super().__init__(f"duplicate client_order_id: {client_order_id}")
        self.client_order_id = client_order_id


class OrderNotFound(BrokerError):
    """The broker does not know the order."""


class AmbiguousSubmission(BrokerError):
    """The outcome of a submission is unknown (timeout / network): the order may or may not exist."""


class InvalidSignalTransition(JEVError):
    """A signal status change that the signal lifecycle does not allow."""
