import json
import time

from kafka import KafkaProducer as BaseProducer
from kafka.errors import (
    IllegalStateError,
    KafkaError,
    KafkaTimeoutError,
    MessageSizeTooLargeError,
    TopicAuthorizationFailedError,
)

from transport_broker.exceptions import (
    AuthorizationError,
    KafkaBrokerTimeoutError,
    MessageSizeError,
    PublisherIllegalError,
)
from transport_broker.publisher.abstract import PublisherAbstract


class KafkaRootPublisher(PublisherAbstract):
    """Connect to the Kafka broker and send messages."""

    def __init__(
            self,
            acks=0,
            api_version=(2, 0, 2),
            retries=3,
            request_timeout_ms=30000,
            max_block_ms=60000,
            *args,
            **kwargs
    ):
        self.acks = acks
        self.api_version = api_version
        self.retries = retries
        self.value_serializer = lambda v: json.dumps(v).encode("utf-8")
        self.request_timeout_ms = request_timeout_ms
        self.max_block_ms = max_block_ms
        super().__init__(*args, **kwargs)

    def send_message(self, message):
        """Send message to the Kafka broker.

        Args:
            message: The message we are sending.

        Returns:
            None:

        Raises:
            TimeoutError: Raises when connect timeout.
            MessageSizeError: Raises when size of messages too large.
            PublisherIllegalError: Raises when publisher is closed forcefully.

        """
        try:
            self.check_connection()

            future = self.producer.send(self.topic, message)
            future.get(timeout=30)
            self.producer.flush()

        except KafkaTimeoutError as e:
            msg = "Timeout while trying to send message."
            self.logger.warning(msg)
            raise KafkaBrokerTimeoutError(msg) from e

        except MessageSizeTooLargeError as e:
            self.logger.error(e)
            raise MessageSizeError(e) from e

        except IllegalStateError as e:
            self.logger.error(e)
            raise PublisherIllegalError(e) from e

        except Exception as e:
            self.logger.warning(f"Unexpected error: {e}")

        else:
            self.logger.debug("Message successfully sent.")
            self.logger.debug(
                f"Broker connection status: {self.connected()}"
            )

        finally:
            self.close()

    def send_list_messages(self, messages):
        """Send list of messages to the broker.

        Args:
            messages (list): List of messages we send.

        Returns:
            None:

        Raises:
            TimeoutError: Raises when connect timeout.
            MessageSizeError: Raises when size of messages too large.
            PublisherIllegalError: Raises when publisher is closed forcefully.

        """
        try:
            self.check_connection()

            self.logger.debug("Starting to send messages.")

            for total_sent, message in enumerate(messages, start=1):
                self.producer.send(self.topic, message)
                if total_sent % self.batch_size_messages == 0:
                    self.producer.flush()
            self.producer.flush()

        except KafkaBrokerTimeoutError as e:
            msg = "Timeout while trying to send messages."
            self.logger.warning(msg)
            raise KafkaBrokerTimeoutError(msg) from e

        except MessageSizeTooLargeError as e:
            self.logger.error(e)
            raise MessageSizeError(e) from e

        except IllegalStateError as e:
            self.logger.error(e)
            raise PublisherIllegalError(e) from e

        except Exception as e:
            self.logger.warning(f"Unexpected error: {e}")

        else:
            self.logger.debug("Messages successfully sent.")
            self.logger.debug(
                f"Broker connection status: {self.connected()}"
            )

        finally:
            self.close()

    def connect(self):
        """Create Kafka Producer client.

        Returns:
            None:

        Raises:
            KeyError: Mapping key not found.
            AuthorizationError: Returned by the broker when the client is not
                authorized to access the requested topic/broker/cluster.

        """
        try:
            self.producer = BaseProducer(
                bootstrap_servers=self.broker,
                value_serializer=self.value_serializer,
                acks=self.acks,
                retries=self.retries,
                api_version=self.api_version,
                request_timeout_ms=self.request_timeout_ms,
                max_block_ms=self.max_block_ms,
            )

        except KeyError as e:
            self.logger.error(e)
            raise

        except TopicAuthorizationFailedError as e:
            self.logger.error(e)
            raise AuthorizationError(e) from e

        except Exception as e:
            self.logger.error("Unexpected Error `%s`", e, exc_info=True)

        else:
            self.logger.debug("Producer client is created.")

    def connected(self):
        """Is the producer connected.

        Returns:
            bool:

        """
        return self.producer.bootstrap_connected() if self.producer else False

    def reconnect(self):
        """Reconnect to the Kafka broker.

        Returns:
            None:

        """
        self.logger.info("Reconnecting to Kafka...")
        if self.connected() or self.producer:
            self.close()
            self.producer = None
        self.connect()

    def check_connection(self):
        """Check if the producer is connected and reconnect while is not.

        Returns:
            None:

        """
        if not self.connected():
            time.sleep(0.5)
            while not self.connected():
                self.logger.info("Waiting for connection...")
                time.sleep(3)
            self.logger.debug("Connection is established.")

    def close(self):
        """Close connection with the Kafka broker.

        Returns:
            None:

        """
        if self.producer:
            try:
                self.producer.close()
                self.producer = None
            except KafkaError as e:
                self.logger.warning(
                    f"An error occurred while trying to close Producer: {e}"
                )
            except Exception as e:
                self.logger.warning(f"Unexpected error: {e}")
            else:
                self.logger.debug("Producer client is closed.")

        else:
            self.logger.info(
                "Producer client didn't created or already closed."
            )
