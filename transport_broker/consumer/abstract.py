from abc import ABC, abstractmethod


class ConsumerAbstract(ABC):
    """Abstract consumer class."""

    @abstractmethod
    def start_consumer(self):
        """Start listening messages from broker."""

    @abstractmethod
    def shutdown(self):
        """Stopping the consumer and completing all tasks."""

    @abstractmethod
    def on_message(self, message):
        """Message processing.

        Args:
            message (dict): Message from the broker.

        """
