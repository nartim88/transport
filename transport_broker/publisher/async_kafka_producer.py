import json
from typing import Optional

from aiokafka.consumer.fetcher import ConsumerRecord  # noqa: F401
from aiokafka.producer import AIOKafkaProducer
from aiokafka.errors import (
    KafkaError,
    KafkaTimeoutError,
)

from transport_broker.exceptions import (
    KafkaBrokerTimeoutError,
)
from transport_broker.publisher.abstract import AsyncPublisherAbstract


class AsyncKafkaRootPublisher(AsyncPublisherAbstract):
    """Async publisher for Kafka."""

    def __init__(
            self,
            broker: str,
            extra_confs: Optional[dict] = None,
            acks: str = "all",
            api_version: str = "auto",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    ):
        self.broker = broker
        self.extra_confs: dict = extra_confs
        self.acks = acks
        self.api_version = api_version
        self.value_serializer = value_serializer
        self.producer: Optional[AIOKafkaProducer] = None

    async def connect(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.broker,
                acks=self.acks,
                api_version=self.api_version,
                value_serializer=self.value_serializer,
                **self.extra_confs or {},
            )
            await self.producer.start()

        except KafkaError as e:
            self.logger.exception(f"Kafka error: {e}")

        except Exception as e:
            self.logger.exception(f"Unexpected error: {e}")

    async def send_message(self, message, topic, wait=True):
        """Send message to the broker.

        Args:
            topic (str): Kafka topic.
            message: The message we are sending.
            wait (bool): Wait result or not.

        Raises:
            KafkaBrokerTimeoutError: Raises when timeout while trying to
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
            self.logger.warning(msg)
            raise KafkaBrokerTimeoutError(msg) from e

    async def send_list_messages(self, messages, topic, wait=True):
        """Send list of messages to the broker.

        Args:
            topic (str): Kafka topic.
            messages (list): List of messages.
            wait (bool): Wait result or not.

        Raises:
            KafkaBrokerTimeoutError: Raises when timeout while trying to
                send messages.

        """
        if not topic:
            self.logger.warning("Topic is not specified.")
            return

        for message in messages:
            await self.send_message(message, topic, wait)

    async def close(self):
        if self.producer:
            await self.producer.stop()
            self.producer = None

    async def reconnect(self):
        await self.close()
        await self.connect()
