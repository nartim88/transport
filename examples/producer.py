from transport_broker import KafkaRootPublisher
from transport_broker.exceptions import (
    KafkaBrokerTimeoutError,
    MessageSizeError,
    PublisherIllegalError,
)

publisher = KafkaRootPublisher(
    broker="localhost:9092",
    topic="base_topic"
)
messages = [f"Hello{i}" for i in range(100)]

# send_message and send_list_messages methods raises some exceptions,
# so you can implement catching these exceptions in your code
try:
    publisher.send_list_messages(messages)

except KeyboardInterrupt:
    pass

except KafkaBrokerTimeoutError:
    pass

except MessageSizeError:
    pass

except PublisherIllegalError:
    pass

except Exception:
    pass
