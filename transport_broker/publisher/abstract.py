import logging
from abc import ABC, abstractmethod


class PublisherAbstract(ABC):
    """Connect to the broker and send messages."""
    logger = logging.getLogger(__name__)

    def __init__(
            self,
            broker,
            topic=None,
            batch_size_messages=25000,
            *args,
            **kwargs
    ):
        """Basic initialization data and connect to broker.

        Args:
            broker (str): Broker service address 'host[:port]'.
            topic (str): Topic where the message will be published.
            batch_size_messages (int): The number of messages we
                send in batches.

        """
        self.broker = broker
        self.topic = topic
        self.batch_size_messages = batch_size_messages
        self.producer = None
        self.connect()

    @abstractmethod
    def send_message(self, *args, **kwargs):
        """Send message to the broker."""

    @abstractmethod
    def connected(self):
        """Is the producer connected."""

    @abstractmethod
    def send_list_messages(self, *args, **kwargs):
        """Send list messages to the broker."""

    @abstractmethod
    def connect(self):
        """ Connect to the broker """

    @abstractmethod
    def reconnect(self):
        """Reconnect to the broker."""

    @abstractmethod
    def close(self):
        """ Close connection to the broker """


class AsyncPublisherAbstract(ABC):
    """Abstract async publisher class."""
    logger = logging.getLogger(__name__)

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
