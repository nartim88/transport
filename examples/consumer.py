import logging
import threading
import time

from transport_broker.consumer.kafka import KafkaRootConsumer
from transport_broker.exceptions import InitializationError

API_VERSION = (2, 8, 0)
GROUP_ID = "threaded_group"
HEARTBEAT_INTERVAL_MS = 6000
HOST = "localhost:9092"
MAX_POLL_INTERVAL_MS = 300_000
MAX_POLL_RECORDS = 1
NEED_THREADS = True
NUM_THREADS = 4
SESSION_TIMEOUT_MS = 18000
TOPIC = "base_topic"
TOPICS = {
    "base_topic_1": 1,
    "base_topic_2": 2,
    "base_topic_3": 3,
}


class CustomConsumer(KafkaRootConsumer):
    def _on_message(self, message):
        time.sleep(1)
        print(message.value)


if __name__ == '__main__':
    logger = logging.getLogger(__name__)

    cons = CustomConsumer(
        api_version=API_VERSION,
        broker=HOST,
        group_id=GROUP_ID,
        heartbeat_interval_ms=HEARTBEAT_INTERVAL_MS,
        max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
        max_poll_records=MAX_POLL_RECORDS,
        need_threads=NEED_THREADS,
        num_threads=NUM_THREADS,
        session_timeout_ms=SESSION_TIMEOUT_MS,
        # topic=TOPIC,
        topics=TOPICS,
    )

    try:
        thread = threading.Thread(
            target=cons.start_consumer,
            name=f"{cons.__class__.__name__}",
        )
        thread.start()
        thread.join()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt")

    except InitializationError as e:
        logger.exception(e)

    finally:
        cons.shutdown()
