import asyncio
import logging

import json
from abc import abstractmethod
from typing import TYPE_CHECKING

from aiokafka import AIOKafkaConsumer
from aiokafka import TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import (
    CommitFailedError,
    GroupAuthorizationFailedError,
    IllegalStateError,
    KafkaError,
    KafkaTimeoutError,
    NoBrokersAvailable,
    TopicAlreadyExistsError,
    TopicAuthorizationFailedError,
    UnknownTopicOrPartitionError,
)

from transport_broker.consumer.abstract import ConsumerAbstract
from transport_broker.exceptions import (
    AuthorizationError,
    ConsumerCalledError,
    InitializationError,
    NewTopicError,
    TopicExistsError,
    UnknownTopicError,
)

if TYPE_CHECKING:
    from aiokafka.consumer.fetcher import ConsumerRecord

MessagesT = dict[TopicPartition, list[ConsumerRecord]]


class AsyncKafkaRootConsumer(ConsumerAbstract):
    """Asynchronous Kafka consumer.

    Attributes:
        MSG_WAIT_TIME: Time in seconds to wait when no messages are.

    Args:
        broker: Broker address host[:port].
        group_id: Kafka Consumer group.
        condition: Asyncio Condition object.
        extra_consumer_configs: Additional AIOKafkaConsumer configuration.
        flags: _no_msgs flags according to topics.
        getmany_configs: `getmany` meth configuration.
        semaphore: Asyncio Semaphore object.
        topic: Topic to subscribe to.
        topics: Topics with priorities.

    """
    logger = logging.getLogger(__name__)

    MSG_WAIT_TIME: float = 1.0

    _instances: set = set()

    def __init__(
            self,
            broker: str,
            group_id: str,
            cached_topics: dict[str, int] | None = None,
            condition: asyncio.Condition | None = None,
            extra_consumer_configs: dict[str] | None = None,
            flags: dict[str, asyncio.Event] | None = None,
            getmany_configs: dict[str] | None = None,
            semaphore: asyncio.Semaphore | None = None,
            topic: str | None = None,
            topics: dict[str, int] | None = None,
    ):
        """Consumer initialization."""
        self.admin_client: AIOKafkaAdminClient | None = None
        self.background_tasks: set = set()
        self.cached_topics: dict[str, int] | None = cached_topics
        self.closing: bool = False
        self.condition: asyncio.Condition | None = condition
        self.consumer: AIOKafkaConsumer | None = None
        self.flags: dict[str, asyncio.Event] | None = flags
        self.need_recon: bool = False
        self.need_stop: asyncio.Event = asyncio.Event()
        self.sec_consumer: type[AsyncSecondaryOneTopicConsumer] | None = None
        self.semaphore: asyncio.Semaphore | None = semaphore
        self.topics: dict[str, int] | None = topics
        self._no_msgs: asyncio.Event = asyncio.Event()

        # consumer confs
        self.broker: str = broker
        self.extra_consumer_configs: dict[str] | None = extra_consumer_configs
        self.getmany_configs: dict[str] | None = getmany_configs
        self.group_id: str = group_id
        self.topic: str | None = topic

        if self.topic and self.topics:
            raise InitializationError(
                "Should be specified only 'topic' or 'topics' parameter."
            )

        if self.topics:
            self.cached_topics = self.topics

    async def start_consumer(self) -> None:
        if not self.topic:
            await self._process_topics()

        else:
            while not self.need_stop.is_set():
                try:
                    await self._start_and_subscribe()

                    if self.flags is not None:
                        self.flags[self.topic] = self._no_msgs

                    messages = await self.consumer.getmany(
                        **self.getmany_configs,
                    )
                    if not messages:
                        self._no_msgs.set()
                        self.logger.debug(f"Condition: {self.condition}")
                        if self.condition:
                            async with self.condition:
                                self.condition.notify_all()
                        wait_time = AsyncKafkaRootConsumer.MSG_WAIT_TIME
                        self.logger.debug(
                            f"[{self.t_name}] - "
                            f"No messages, sleep {wait_time}s"
                        )

                        await asyncio.sleep(wait_time)
                        await self._block_consumer_if_cant_process()
                        continue

                    msgs_num = sum(map(len, messages.values()))
                    self.logger.debug(
                        f"""[{self.t_name}]: 
                        Got messages - {msgs_num}, 
                        Can process? - {self._can_process()}, 
                        Flags: {self.flags}"""
                    )

                    self._no_msgs.clear()
                    await self._block_consumer_if_cant_process()
                    await self._consume_messages(messages)
                    self.logger.debug(
                        f"""[{self.t_name}]: Batch of messages is consumed. 
                        Total background tasks amount: 
                        {len(self.background_tasks)} 
                        All Python tasks: {len(asyncio.all_tasks())}"""
                    )

                except CommitFailedError:
                    self.logger.exception(
                        f"[{self.t_name}]: "
                        f"Commit error, please reconnect to kafka group."
                    )
                    self.need_recon = True

                except KafkaTimeoutError as e:
                    self.logger.exception(
                        f"[{self.t_name}]: Timeout error: {e}"
                    )
                    self.need_recon = True

                except NewTopicError:
                    self.logger.info(
                        f"[{self.t_name}]: "
                        f"Reconnect to consumer, topic is recreated."
                    )
                    self.need_recon = True

                except NoBrokersAvailable:
                    self.logger.info(
                        f"[{self.t_name}]: "
                        f"Reconnect to consumer, broker doesn't available."
                    )
                    self.need_recon = True

                except AssertionError as e:
                    self.logger.exception(
                        f"[{self.t_name}]: AssertionError: {e}"
                    )

                except TopicExistsError as e:
                    self.logger.warning(f"[{self.t_name}]: {e}")

                except RuntimeError as e:
                    self.logger.exception(
                        f"[{self.t_name}]: Runtime error: {e}"
                    )

                except ConsumerCalledError as e:
                    self.logger.warning(f"[{self.t_name}]: {e}")

                except Exception as e:
                    self.logger.exception(
                        f"[{self.t_name}]: Unexpected Error: {e}"
                    )
                    self.need_recon = True

    async def _block_consumer_if_cant_process(self) -> None:
        """Block consumer.

        Consumer will be blocking until consumers with higher
        priority topics have messages.

        """
        if self.condition and not self._can_process():
            async with self.condition:
                self.logger.debug(
                    f"{self.t_name} is locked."
                )
                await self.condition.wait_for(self._can_process)
                self.logger.debug(
                    f"{self.t_name} is released."
                )

    async def _process_topics(self) -> None:
        """Start separate consumer for each topic if list is given."""
        if not self.topic and not self.condition:
            self.condition = asyncio.Condition()
            self.logger.debug(f"Condition obj is created: {self.condition}")
            self.flags = {}

        topics = self.topics or self.cached_topics

        sorted_topics_by_priority = dict(
            sorted(topics.items(), key=lambda x: x[1])
        )

        try:
            async with asyncio.TaskGroup() as group:
                for topic in sorted_topics_by_priority:
                    consumer = self.sec_consumer(
                        primary_consumer=self,
                        broker=self.broker,
                        cached_topics=topics,
                        topic=topic,
                        group_id=topic,
                        condition=self.condition,
                        flags=self.flags,
                        semaphore=self.semaphore,
                        extra_consumer_configs=self.extra_consumer_configs,
                        getmany_configs=self.getmany_configs,
                    )
                    task: asyncio.Task = group.create_task(
                        coro=consumer.start_consumer(),
                        name=f"{topic}_consumer",
                    )
                    self.logger.debug(f"Task {task.get_name()} is started.")
                    await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            self.logger.debug(f"Task {self.t_name} is cancelled.")

    async def _consume_messages(self, messages: MessagesT) -> None:
        """Consume messages from the poll data.

        Args:
            messages: Data from a consumer `getmany` method.

        """
        for topic_partition, message_list in messages.items():
            for message in message_list:

                if self.need_stop.is_set():
                    self.logger.debug(f"Need stop: {self.need_stop.is_set()}")
                    break

                await self._ack(message, topic_partition)

                self.logger.debug(
                    f"[{self.t_name}]: Starting consuming a message: "
                    f"{json.dumps(message.value)[:300]}"
                )

                if self.semaphore:
                    await self.semaphore.acquire()
                    self.logger.debug(f"Semaphore state: {self.semaphore}")

                task = asyncio.create_task(self.on_message(message))
                self.background_tasks.add(task)
                task.add_done_callback(self._process_after_task_done)

                self.logger.debug(
                    f"Task {task.get_name()} running. "
                    f"Tasks amount: {len(self.background_tasks)}"
                )

                last_offset = await self.consumer.end_offsets(
                    [topic_partition],
                )
                last_offset = last_offset.get(topic_partition)
                self.logger.info(
                    "[current_offset:max_offset] - "
                    f"[{message.offset}:{last_offset}]"
                )

    def _process_after_task_done(self, task: asyncio.Task) -> None:
        """Remove task from the background tasks set and release semaphore."""
        self.background_tasks.discard(task)
        self.logger.debug(
            f"Task {task.get_name()} is done. "
            f"Tasks amount: {len(self.background_tasks)}"
        )
        if self.semaphore:
            self.semaphore.release()

    async def _ack(
            self,
            message: ConsumerRecord,
            topic_partition: TopicPartition,
    ) -> None:
        """Commit the current offset.

        Args:
            message: Current received message.
            topic_partition: Current topic partition.

        """
        self.consumer.seek(topic_partition, message.offset + 1)
        await self.consumer.commit()

    async def _close(self) -> None:
        """Close Kafka Consumer client."""
        if not self.consumer:
            self.logger.info(
                f"[{self.t_name}]: "
                f"Consumer client didn't created or already closed."
            )
            return

        try:
            self.logger.debug(
                f"[{self.t_name}]: "
                f"Start closing consumer '{self.__class__.__name__}'"
            )
            await self._close_procedures()

        except KafkaError as e:
            self.logger.warning(
                f"[{self.t_name}]: "
                f"An error occurred while trying to close Consumer: {e}"
            )

        else:
            self.logger.debug(
                f"Consumer '{self.__class__.__name__}' is closed."
            )

    async def _close_procedures(self) -> None:
        if self.consumer:
            self.consumer.unsubscribe()
            await self.consumer.stop()
        if self.admin_client:
            await self.admin_client.close()
        self.consumer = None
        self.admin_client = None
        self.consuming = False
        self.closed = True

    def _subscribe_topic(self, topic: str) -> None:
        """Subscribe to a topic.

        Args:
            topic: The topic to subscribe to.

        Raises:
            ConsumerCalledError: Raises when consumer has already called.
            Exception: Unexpected error.

        """
        try:
            self.consumer.subscribe([topic])
            self.logger.info(f"[{self.t_name}]: Subscribed to topic '{topic}'")
        except IllegalStateError as e:
            raise ConsumerCalledError(e) from e
        except Exception as e:
            self.logger.error(
                f"[{self.t_name}]: "
                "Unexpected Error `%s`", e, exc_info=True
            )
            raise

    async def _connect(self) -> None:
        """Connect to the Kafka broker.

        Raises:
            AuthorizationError: Returned by the broker when the client is not
                authorized to access the requested topic/broker/cluster.

        """
        try:
            self.consumer = AIOKafkaConsumer(
                bootstrap_servers=self.broker,
                group_id=self.group_id,
                **self.extra_consumer_configs,
            )
            await self.consumer.start()
            self.closed = False
        except (
            TopicAuthorizationFailedError,
            GroupAuthorizationFailedError
        ) as e:
            raise AuthorizationError(e) from e
        else:
            self.logger.debug(
                f"Consumer client '{self.consumer}' is connected "
                f"to the broker."
            )

    async def shutdown(self) -> None:
        """Stop the consumer and complete all tasks."""
        if self._instances and self not in self._instances:
            await self._end_child_consumers()
        await self._shutdown_processes()

    async def _end_child_consumers(self) -> None:
        for instance in self._instances:
            instance: AsyncKafkaRootConsumer
            await instance.shutdown()
            instance.logger.info(
                f"Consumer service '{instance.__class__.__name__}' "
                f"is shutting down..."
            )

    async def _shutdown_processes(self) -> None:
        """Procedures for graceful consumer shutdown."""
        if not self.closing:
            self.need_stop.set()
            self.closing = True
            await self._wait_all_tasks()
            await self._close()
            self.closing = False
            self.logger.info(
                f"Shutdown consumer service '{self.__class__.__name__}': OK"
            )

    async def _wait_all_tasks(self) -> None:
        if not self.background_tasks:
            self.logger.debug(f"[{self.t_name}]: No tasks to wait.")
            return

        if current_task := asyncio.current_task():
            self.background_tasks.discard(current_task)

        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.logger.debug(f"[{self.t_name}]: All tasks are done.")

    async def create_topics(
            self,
            topic_names: list[str],
            num_partitions: int = 1,
    ) -> None:
        """Create new topics if they don't exist in Kafka broker.

        Args:
            topic_names: List of topics.
            num_partitions: Number of partitions.

        Raises:
            TopicExistsError: Raises when topic already exist on this broker.
            Exception: Unexpected error.

        """
        if not self.admin_client:
            await self._create_admin_client()

        if not self.consumer:
            await self._connect()

        existing_topic_list = await self.consumer.topics()
        self.logger.debug(
            f"[{self.t_name}]: Existing topics in the broker: "
            f"{existing_topic_list}"
        )
        topic_list = []

        for topic in topic_names:
            if topic not in existing_topic_list:
                self.logger.info(
                    f"[{self.t_name}]: The given topic '{topic}' "
                    f"doesn't exists in the broker."
                )
                topic_list.append(
                    NewTopic(
                        name=topic,
                        num_partitions=num_partitions,
                        replication_factor=1,
                    ),
                )
            else:
                self.logger.info(
                    f"[{self.t_name}]: The given topic '{topic}' "
                    f"already exists in the broker."
                )

        try:
            if topic_list:
                await self.admin_client.create_topics(new_topics=topic_list)
                self.logger.info(
                    f"[{self.t_name}]: Topic created successfully."
                )
            else:
                self.logger.info(
                    f"[{self.t_name}]: Nothing to create. "
                    f"All topics exist in the broker."
                )

        except TopicAlreadyExistsError as e:
            self.logger.warning(
                f"[{self.t_name}]: Topic already exists."
            )
            raise TopicExistsError from e

        except Exception as e:
            self.logger.exception(
                f"[{self.t_name}]: Unexpected error occurred while "
                f"creating topics : {e}"
            )
            raise

    async def delete_topics(self, topic_names: list[str]) -> None:
        """Delete given topics.

        Args:
            topic_names: List of topics.

        """
        if not self.admin_client:
            await self._create_admin_client()

        try:
            await self.admin_client.delete_topics(topics=topic_names)
            self.logger.info(
                f"[{self.t_name}]: Topics: {topic_names} Deleted Successfully"
            )
        except UnknownTopicOrPartitionError as e:
            self.logger.error(
                f"[{self.t_name}]: Topics: {topic_names} Doesn't Exist"
            )
            raise UnknownTopicError from e
        except Exception as e:
            self.logger.exception(
                f"[{self.t_name}]: Unexpected Error {e}")
            raise

    async def _create_admin_client(self) -> None:
        """Create and start kafka admin client."""
        self.admin_client = AIOKafkaAdminClient(bootstrap_servers=self.broker)
        await self.admin_client.start()

    def _can_process(self) -> bool:
        """Returns True if consumers with higher priority have no messages."""
        if not self.condition:
            return True

        current_topic_priority_value = self.cached_topics.get(self.topic)
        flags_values = []

        for topic_name, topic_priority_value in self.cached_topics.items():
            if current_topic_priority_value > topic_priority_value:

                no_msgs_flag = self.flags.get(topic_name)

                if no_msgs_flag:
                    if no_msgs_flag.is_set():
                        flags_values.append(True)
                    else:
                        flags_values.append(False)

        return all(flags_values)

    async def _start_and_subscribe(self) -> None:
        """Connection and subscribing processes."""
        if self.need_recon:
            await self._reconnect()

        if not self.consumer:
            await self._connect()

        if self.topic and self.topic not in await self.consumer.topics():
            await self.create_topics([self.topic])

        if (
                self.consumer.subscription() is None or
                self.topic not in self.consumer.subscription()
        ):
            self._subscribe_topic(self.topic)

    async def _reconnect(self) -> None:
        """Reconnect to the broker."""
        if self.consumer:
            await self._close()
        await self._connect()
        self.need_recon = False

    @property
    def t_name(self) -> str:
        """Get current task name."""
        return asyncio.current_task().get_name()

    @property
    def instances(self) -> set:
        """Get all consumer instances if list of topics was given."""
        return AsyncKafkaRootConsumer._instances

    @abstractmethod
    async def on_message(self, message: ConsumerRecord) -> None:
        """Message processing.

        Args:
            message: Message from a kafka broker.

        """


class AsyncSecondaryOneTopicConsumer(AsyncKafkaRootConsumer):
    """Separated consumer for each topic if topic list is given."""

    def __init__(self, primary_consumer, *args, **kwargs):
        """Initialization.

        Args:
            primary_consumer (AsyncKafkaRootConsumer): Instance of a
                calling class.

        """
        super().__init__(*args, **kwargs)
        self.primary_consumer = primary_consumer
        self._instances.add(self)

    async def on_message(self, message):
        return await self.primary_consumer.on_message(message)


class AsyncKafkaConsumerPartitions:
    """Async kafka consumer with partition prioritization."""
    logger = logging.getLogger(__name__)

    def __init__(
            self,
            broker,
            group_id,
            partitions_with_prio,
            topic,
            extra_broker_configs: dict,
            max_records=500,
    ):
        self.broker = broker
        self.group_id = group_id
        self.consumer: AIOKafkaConsumer | None = None
        self.partitions_with_prio = partitions_with_prio
        self.topic = topic
        self.max_records = max_records
        self.extra_broker_configs: dict = extra_broker_configs

    async def start_consumer(self):
        """Assign consumer to a partition and start consuming.

        Warnings:
            Not tested!

        """
        if not self.consumer:
            await self.connect()

        while True:
            partition = await self._get_partition()
            self.assign_partitions([partition])
            batch = await self.consumer.getmany(
                max_records=self.max_records)
            messages = batch.get(partition)
            self.logger.info(f"Got {len(messages)} messages.")
            await self._ack(messages, partition)
            await self.process_messages(messages)

    async def connect(self):
        """Connect to a Kafka broker."""
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.broker,
            group_id=self.group_id,
            **self.extra_broker_configs,
        )
        await self.consumer.start()
        self.logger.debug(
            f"Consumer client '{self.consumer}' is connected to broker")

    def assign_partitions(self, partitions):
        """Assign consumer to partitions.

        Args:
            partitions (list): List of TopicPartition objects.

        """
        self.consumer.assign(partitions)
        self.logger.info("Consumer is assigned to partitions.")

    async def process_messages(self, messages):
        raise NotImplementedError

    async def _count_msgs_in_partition(self, partition):
        """Count messages in the partition.

        Args:
            partition (TopicPartition): Partition to count messages.

        Returns:
            int: Number of messages in the partition.

        """
        offset = await self.consumer.position(partition)
        last_offset = await self.consumer.end_offsets([partition])
        return last_offset.get(partition) - offset

    async def _get_partition(self):
        sorted_partitions = await self._sort_partitions_by_priority()
        for partition_info in sorted_partitions:
            count = partition_info["count"]
            if count > 0:
                return partition_info["partition"]

    async def _sort_partitions_by_priority(self):
        """Sort partitions by priority.

        Returns:
            list: Sorted list of partitions.

        """
        partitions = []
        for partition_info in self.partitions_with_prio:
            partition = TopicPartition(self.topic, partition_info["partition"])
            partitions.append(
                {
                    "partition": partition,
                    "priority": partition_info["priority"],
                    "count": await self._count_msgs_in_partition(partition),
                }
            )
        return sorted(partitions, key=lambda x: x["priority"])

    async def _ack(self, messages, topic_partition):
        """Commit the current offset.

        Args:
            messages (list[ConsumerRecord]): Current received messages.
            topic_partition (TopicPartition): Current topic partition.

        """
        for message in messages:
            self.consumer.seek(topic_partition, message.offset + 1)
            await self.consumer.commit()

    async def shutdown(self):
        """Stop consumer."""
        if self.consumer:
            await self.consumer.stop()
            self.logger.info(
                f"Shutdown consumer service "
                f"'{self.__class__.__name__}': OK"
            )
