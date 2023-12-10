from abc import ABC, abstractmethod


class PublisherAbstract(ABC):
    """Abstract async publisher class."""

    @abstractmethod
    async def send_message(self, *args, **kwargs):
        """Send message to the broker."""

    @abstractmethod
    async def send_list_messages(self, *args, **kwargs):
        """Send list messages to the broker."""

    @abstractmethod
    def connect(self):
        """Connect to the broker."""

    @abstractmethod
    def reconnect(self):
        """Reconnect to the broker."""

    @abstractmethod
    async def close(self):
        """Close connection to the broker."""
