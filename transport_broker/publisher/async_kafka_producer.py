import json
import logging

from aiokafka.producer import AIOKafkaProducer
from aiokafka.errors import (
    KafkaError,
    KafkaTimeoutError,
)

from transport_broker.exceptions import (
    BrokerTimeoutError,
    TransportException,
)
from transport_broker.publisher.abstract import PublisherAbstract


class AsyncKafkaRootPublisher(PublisherAbstract):
    """Async publisher for Kafka."""

    logger = logging.getLogger(__name__)

    DEFAULT_CONFS: dict[str] = {
        "acks": "all",
        "api_version": "auto",
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    }

    def __init__(
            self,
            broker: str,
            extra_confs: dict[str] | None = None,
    ) -> None:
        self.broker: str = broker
        self.extra_confs: dict[str] = extra_confs
        self.producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        """Connect to the broker.

        Raises:
            TransportException:

        """
        try:
            self.extra_confs |= AsyncKafkaRootPublisher.DEFAULT_CONFS
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.broker,
                **self.extra_confs or {},
            )
            await self.producer.start()

        except KafkaError as e:
            raise TransportException(f"Kafka error: {e}")

        except Exception:
            raise

    async def send_message(
            self,
            message: str,
            topic: str,
            wait: bool = True,
    ) -> None:
        """Send message to the broker.

        Args:
            topic: Kafka topic.
            message: The message we are sending.
            wait: Wait result or not.

        Raises:
            BrokerTimeoutError: Raises when timeout while trying to
                send message.

        """
        if not topic:
            self.logger.warning("Topic is not specified.")
            return

        try:
            if wait:
                await self.producer.send_and_wait(topic, message)
            else:
                await self.producer.send(topic, message)

        except KafkaTimeoutError as e:
            msg = "Timeout while trying to send message."
            raise BrokerTimeoutError(f"{msg}: {e}")

    async def send_list_messages(
            self,
            messages: list[str],
            topic: str,
            wait: bool = True,
    ) -> None:
        """Send list of messages to the broker.

        Args:
            topic: Kafka topic.
            messages: List of messages.
            wait: Wait result or not.

        Raises:
            BrokerTimeoutError: Raises when timeout while trying to
                send messages.

        """
        if not topic:
            self.logger.warning("Topic is not specified.")
            return

        try:
            for message in messages:
                await self.send_message(message, topic, wait)

        except BrokerTimeoutError:
            raise

    async def close(self) -> None:
        if self.producer:
            await self.producer.stop()
            self.producer = None

    async def reconnect(self) -> None:
        await self.close()
        await self.connect()
