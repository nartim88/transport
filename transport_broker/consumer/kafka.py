import json
import logging
import time
from abc import abstractmethod
from threading import BoundedSemaphore, Condition, Thread, active_count

from kafka import KafkaConsumer as BaseConsumer
from kafka import OffsetAndMetadata, TopicPartition  # noqa: F401
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.consumer.fetcher import ConsumerRecord  # noqa: F401
from kafka.errors import (
    CommitFailedError,
    GroupAuthorizationFailedError,
    IllegalStateError,
    KafkaTimeoutError,
    NoBrokersAvailable,
    TopicAlreadyExistsError,
    TopicAuthorizationFailedError,
    UnknownTopicOrPartitionError, KafkaError,
)

from transport_broker.consumer.abstract import ConsumerAbstract
from transport_broker.exceptions import (
    AuthorizationError,
    ConsumerCalledError,
    NewTopicError,
    TopicExistsError,
    UnknownTopicError,
)


class KafkaRootConsumer(ConsumerAbstract):
    """Receiving messages from kafka, processing them.

    The ability to work in multithreading mode. Interact
    from the outside, for example from another stream.
    Also, the ability to create and delete topics.

    """
    _instances = set()

    def __init__(
            self,
            group_id,
            api_version=(2, 0, 2),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            heartbeat_interval_ms=3000,
            max_poll_interval_ms=300000,
            max_poll_records=500,
            poll_timeout_ms=300,
            poll_update_offsets=False,
            session_timeout_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode("ascii")),
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.admin_client = None
        self.api_version = api_version
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.group_id = group_id
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.max_poll_interval_ms = max_poll_interval_ms
        self.max_poll_records = max_poll_records
        self.poll_timeout_ms = poll_timeout_ms
        self.poll_update_offsets = poll_update_offsets
        self.session_timeout_ms = session_timeout_ms
        self.value_deserializer = value_deserializer

    def start_consumer(self):  # noqa: C901
        """Subscribe to the topic and process messages.

        As soon as we receive the message, we commit it and send it
        for processing. We are waiting for the stream to be released.

        """
        if self.need_threads and not self._semaphore:
            self._semaphore = BoundedSemaphore(self.max_workers)
            self.logger.debug(f"Semaphore is created: {self._semaphore}")

        if not self.topic:
            self._process_topics()

        else:
            while not self.need_stop:
                try:
                    self._start_and_subscribe()

                    if self._flags is not None:
                        with self._lock:
                            self._flags[self.topic] = self._no_msgs

                    messages = self.consumer.poll(
                        timeout_ms=self.poll_timeout_ms,
                        update_offsets=self.poll_update_offsets,
                    )
                    if not messages:

                        # If current consumer has no messages set flag to
                        # True value.
                        self._no_msgs.set()

                        if self._condition:
                            with self._condition:
                                self._condition.notify_all()

                        self.logger.debug(
                            f"Semaphore: {self._semaphore}. "
                            f"Threads: {active_count()}. "
                            f"No messages, sleep {self.message_wait_time}s"
                        )

                        time.sleep(self.message_wait_time)
                        continue

                    msgs_num = sum(map(len, messages.values()))
                    self.logger.debug(
                        f"Got messages - {msgs_num}. "
                        f"Can process? - {self._can_process()}. "
                        f"Flags: {self._flags}"
                    )

                    self._no_msgs.clear()

                    # Block consumer thread while consumers with higher
                    # priority topics have messages.
                    if self._condition and not self._can_process():
                        with self._condition:
                            self.logger.debug("Thread is locked.")
                            self._condition.wait_for(self._can_process)
                            self.logger.debug("Thread is released.")

                    self._consume_messages(messages)

                    self._check_tasks()

                except CommitFailedError:
                    self.logger.exception(
                        "Commit error, please reconnect to kafka group"
                    )
                    self.need_recon = True

                except KafkaTimeoutError as e:
                    self.logger.exception(f"Timeout error: {e}")
                    self.need_recon = True

                except NewTopicError:
                    self.logger.info(
                        "Reconnect to consumer, topic is recreated"
                    )
                    self.need_recon = True

                except NoBrokersAvailable:
                    self.logger.info(
                        "Reconnect to consumer, broker doesn't available")
                    time.sleep(self.error_wait_time)
                    self.need_recon = True

                except AssertionError as e:
                    self.logger.exception(f"AssertionError: {e}")

                except TopicExistsError as e:
                    self.logger.warning(e)

                except RuntimeError as e:
                    self.logger.exception(f"Runtime error: {e}")

                except ConsumerCalledError as e:
                    self.logger.warning(e)

                except Exception as e:
                    self.logger.exception(f"Unexpected Error: {e}")
                    self.need_recon = True

        if not self.closed:
            self.shutdown()

    def _process_topics(self):
        if not self.topic and not self._condition:
            self._condition = Condition()
            self._flags = {}

        topics = self.topics or self.cached_topics

        sorted_topics_by_priority = dict(
            sorted(topics.items(), key=lambda x: x[1])
        )
        threads = []
        for topic in sorted_topics_by_priority:
            consumer = SecondaryOneTopicConsumer(
                klass=self,
                api_version=self.api_version,
                auto_offset_reset=self.auto_offset_reset,
                broker=self.broker,
                cached_topics=topics,
                enable_auto_commit=self.enable_auto_commit,
                group_id=self.group_id,
                heartbeat_interval_ms=self.heartbeat_interval_ms,
                max_poll_interval_ms=self.max_poll_interval_ms,
                max_poll_records=self.max_poll_records,
                need_threads=self.need_threads,
                num_threads=self.max_workers,
                poll_timeout_ms=self.poll_timeout_ms,
                poll_update_offsets=self.poll_update_offsets,
                session_timeout_ms=self.session_timeout_ms,
                topic=topic,
                value_deserializer=self.value_deserializer,
                semaphore=self._semaphore,
                condition=self._condition,
                flags=self._flags,
            )
            thread = Thread(
                target=consumer.start_consumer,
                name=f"{topic}_consumer",
            )
            thread.start()
            threads.append(thread)
            self.logger.debug(
                f"Thread {thread.ident} ({thread.name}) started."
            )
            time.sleep(0.5)

        for thread in threads:
            thread.join()

    def _consume_messages(self, messages):
        """Consume messages from the poll data.

        Args:
            messages (dict[TopicPartition, list[ConsumerRecord]]): Data from
                a consumer poll method.

        Returns:
            None:

        """
        for topic_partition, message_list in messages.items():
            for message in message_list:

                if self.need_stop:
                    break

                self._ack(message, topic_partition)

                self.logger.debug(
                    "Starting consuming a message: "
                    f"{json.dumps(message.value)[:300]}\n"
                    f"Active threads: {active_count()}"
                )

                if not self.need_threads:
                    self.task_done = False
                    self._on_message(message)
                    self.task_done = True

                else:
                    thread = ConsumeMsgThread(
                        target=self._on_message,
                        args=(message, ),
                        name=f"{self.topic}_msg_process",
                        semaphore=self._semaphore,
                    )
                    self.futures.add(thread)
                    thread.start()

                    self.logger.debug(f"len futures: {len(self.futures)}")
                    end_offset = (
                        self.consumer.end_offsets([topic_partition])
                        .get(topic_partition)
                    )
                    self.logger.info(
                        "[current_offset:max_offset]:"
                        f"[{message.offset}:{end_offset}]"
                    )

    def _ack(self, message, topic_partition):
        """Commit the current offset.

        Args:
            message (ConsumerRecord): Current received message.
            topic_partition (TopicPartition): Current topic partition.

        Returns:
            None:

        Raises:
            AssertionError:

        """
        try:
            with self._lock:
                self.consumer.seek(topic_partition, message.offset + 1)
                self.consumer.commit()

        except AssertionError as e:
            raise AssertionError("Error occurred while trying to Ack.") from e

        else:
            self.logger.debug(f"Ack offset: {message.offset}")

    def _close(self):
        """Close Kafka Consumer client."""
        if self.consumer:
            try:
                self.logger.debug(
                    f"Start closing consumer '{self.__class__.__name__}'"
                )
                self._close_procedures()
            except KafkaError as e:
                self.logger.warning(
                    f"An error occurred while trying to close Consumer: {e}"
                )
            else:
                self.logger.debug(
                    f"Consumer '{self.__class__.__name__}' is closed."
                )
        else:
            self.logger.info(
                "Consumer client didn't created or already closed.")

    def _close_procedures(self):
        self.consumer.unsubscribe()
        self.consumer.close()
        self.consumer = None
        self.consuming = False
        self.closed = True

    def _subscribe_topic(self, topic):
        """Subscribe to a topic.

        Args:
            topic (str): The topic to subscribe to.

        Returns:
            None:

        Raises:
            ConsumerCalledError: Raises when consumer has already called.
            Exception: Unexpected error.

        """
        try:
            self.consumer.subscribe(topic)
            self.logger.info("Consumer is listening...")
        except IllegalStateError as e:
            raise ConsumerCalledError(e) from e
        except Exception as e:
            self.logger.error("Unexpected Error `%s`", e, exc_info=True)
            raise

    def _connect(self):
        """Connect to a Kafka cluster.

        Returns:
            None:

        Raises:
            AuthorizationError: Returned by the broker when the client is not
                authorized to access the requested topic/broker/cluster.

        """
        try:
            self.consumer = BaseConsumer(
                api_version=self.api_version,
                auto_offset_reset=self.auto_offset_reset,
                bootstrap_servers=self.broker,
                enable_auto_commit=self.enable_auto_commit,
                group_id=self.group_id,
                heartbeat_interval_ms=self.heartbeat_interval_ms,
                max_poll_interval_ms=self.max_poll_interval_ms,
                max_poll_records=self.max_poll_records,
                session_timeout_ms=self.session_timeout_ms,
                value_deserializer=self.value_deserializer,
            )
            self.closed = False
        except (
            TopicAuthorizationFailedError,
            GroupAuthorizationFailedError
        ) as e:
            raise AuthorizationError(e) from e
        else:
            self.logger.debug(f"Consumer client is created: {self.consumer}")

    def _check_tasks(self):
        """Check the status of all tasks.
        If the status is in self.states, then delete the task
        """
        for thread in self.futures.copy():
            if not thread.is_alive():
                self.futures.remove(thread)

    def _wait_all_tasks(self):
        """Waiting for the completion of all tasks.

        Returns:
            None:

        """
        if self.need_threads:
            self.logger.debug("Waiting for completion of all tasks...")
            while self.futures:
                self._check_tasks()
                time.sleep(self.thread_waiting_time)
            self.logger.debug("All tasks have been completed.")
        else:
            while not self.task_done:
                time.sleep(self.thread_waiting_time)

    def shutdown(self):
        """Stopping the consumer and completing all tasks"""
        if self._instances:
            self._end_child_threads()
        self._shutdown_processes()

    def _shutdown_processes(self):
        """Procedures for graceful consumer shutdown.

        Returns:
            None:

        """
        if not self.closing:
            self.need_stop = True
            self.closing = True
            self._wait_all_tasks()
            self._close()
            self.closing = False
            self.logger.info(
                f"Shutdown consumer service "
                f"'{self.__class__.__name__}': OK"
            )

    def _create_topics(self, topic_names, num_partitions=1):
        """Create a new topics if they don't exist in Kafka broker.

        Returns:
            None:

        Raises:
            TopicExistsError: Raises when topic already exist on this broker.
            Exception: Unexpected error.

        """
        if not self.admin_client:
            self._create_admin_client()

        if not self.consumer:
            self._connect()

        existing_topic_list = self.consumer.topics()
        self.logger.debug(
            f"Existing topics in the broker: {existing_topic_list}"
        )
        topic_list = []

        for topic in topic_names:
            if topic not in existing_topic_list:
                self.logger.info(
                    f"The given topic '{topic}' doesn't exists in the broker."
                )
                topic_list.append(
                    NewTopic(
                        name=topic,
                        num_partitions=num_partitions,
                        replication_factor=1,
                    )
                )
            else:
                self.logger.info(
                    f"The given topic '{topic}' already exists in the broker."
                )

        try:
            if topic_list:
                self.admin_client.create_topics(
                    new_topics=topic_list, validate_only=False
                )
                self.logger.info("Topic created successfully.")
            else:
                self.logger.info(
                    "Nothing to create. All topics exist in the broker."
                )

        except TopicAlreadyExistsError as e:
            self.logger.warning(
                "Topic already exists."
            )
            raise TopicExistsError from e

        except Exception as e:
            self.logger.exception(
                f"Unexpected error occurred while creating topics : {e}"
            )
            raise

    def _delete_topics(self, topic_names):
        if not self.admin_client:
            self._create_admin_client()
        try:
            self.admin_client.delete_topics(topics=topic_names)
            self.logger.info(f"Topics: {topic_names} Deleted Successfully")
        except UnknownTopicOrPartitionError as e:
            self.logger.error(f"Topics: {topic_names} Doesn't Exist")
            raise UnknownTopicError from e
        except Exception as e:
            self.logger.error("Unexpected Error `%s`", e, exc_info=True)
            raise

    def _create_admin_client(self):
        self.admin_client = KafkaAdminClient(bootstrap_servers=[self.broker])

    def _can_process(self):
        if not self._condition:
            return True

        self.consumer.poll(max_records=1)

        current_topic_priority_value = self.cached_topics.get(self.topic)
        flags_values = []

        for topic_name, topic_priority_value in self.cached_topics.items():
            if current_topic_priority_value > topic_priority_value:

                with self._lock:
                    no_msgs_flag = self._flags.get(topic_name)

                if no_msgs_flag:
                    if no_msgs_flag.is_set():
                        flags_values.append(True)
                    else:
                        flags_values.append(False)

        return all(flags_values)

    def _start_and_subscribe(self):
        if self.need_recon:
            self._reconnect()

        if not self.consumer:
            self._connect()

        if self.topic and self.topic not in self.consumer.topics():
            self._create_topics([self.topic])

        if (
                self.consumer.subscription() is None or
                self.topic not in self.consumer.subscription()
        ):
            self._subscribe_topic(self.topic)

    def _end_child_threads(self):
        """Shutdown all child consumers."""
        for consumer in self._instances:
            if isinstance(consumer, SecondaryOneTopicConsumer):
                consumer._shutdown_processes()

    @abstractmethod
    def _on_message(self, message):
        """Message processing.

        Args:
            message (ConsumerRecord): Message from a kafka broker.

        """


class SecondaryOneTopicConsumer(KafkaRootConsumer):
    """Separated consumer for each topic if topic list is given."""
    def __init__(self, klass, *args, **kwargs):
        """Initialization.

        Args:
            klass (KafkaRootConsumer): Instance of a calling class.

        """
        super().__init__(*args, **kwargs)
        self.klass = klass
        self._instances.add(self)

    def _on_message(self, message):
        return self.klass._on_message(message)


class ConsumeMsgThread(Thread):
    """Custom Thread using semaphore."""
    def __init__(self, group=None, target=None, name=None, args=(),
                 kwargs=None, *, daemon=None, semaphore=None):
        """Initialization.

        Args:
            semaphore (Semaphore): Bounded semaphore object.

        """
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self._semaphore = semaphore
        self._logger = logging.getLogger(__name__)

    def start(self):
        try:
            self._semaphore_acquire()
            super().start()
        except Exception as e:
            self._logger.warning(e)
            self._semaphore_release()

    def run(self):
        start_time = time.perf_counter()
        try:
            self._logger.debug("The thread is running.")
            self._logger.debug(
                f"Semaphore available limit: {self._semaphore._value}/"  # noqa
                f"{self._semaphore._initial_value}"  # noqa
            )
            super().run()
        finally:
            work_time = round(time.perf_counter() - start_time, 4)
            self._logger.debug(f"Thread finished [time: {work_time}].")
            self._semaphore_release()

    def _semaphore_acquire(self):
        if self._semaphore:
            self._semaphore.acquire()

    def _semaphore_release(self):
        if self._semaphore:
            self._semaphore.release()
