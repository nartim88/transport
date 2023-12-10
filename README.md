# Transport broker
A small library for simplified use of message brokers

### WARNING
AIO producer and consumer work only with Python 3.11.

### Installation
Add to `.env` file or export to environment `GITHUB_USERNAME` and `GITHUB_PASSWORD` 
variables with your GitHub credentials.  
Install package from GitHab:
```bash
pip install -U git+https://${GITHUB_USERNAME}:${GITHUB_PASSWORD}@github.com/nartim88/transport.git@master
```

### Adding to `requirements.txt`
You can add this package to your `requirements.txt` file:
```bash
echo "transport_broker @ git+https://${GITHUB_USERNAME}:${GITHUB_PASSWORD}@github.com/nartim88/transport.git@master" >> requirements.txt
```

### Kafka transport usage
Run kafka consumer:

```python
import asyncio

from transport_broker import AsyncKafkaRootConsumer


class CustomConsumer(AsyncKafkaRootConsumer):
    async def on_message(self, message):
        print(message.value)


cons = CustomConsumer(
    broker="localhost:9092",
    topic="base_topic",
    group_id="base_group",
)
asyncio.run(cons.start_consumer())

```

Send message to the Kafka:

```python
import asyncio

from transport_broker import AsyncKafkaRootPublisher


async def main():
    publisher = AsyncKafkaRootPublisher(broker="localhost:9092")

    await publisher.connect()
    await publisher.send_message(topic="base_topic", message="Hello!")
    await publisher.close()

asyncio.run(main())

```
