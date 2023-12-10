class TransportException(BaseException):
    """ Basic exception class """


class KafkaBrokerTimeoutError(TransportException):
    """ Raises when connect timeout """


class MessageSizeError(TransportException):
    """ Raises when size of messages too large """


class AuthorizationError(TransportException):
    """
    Returned by the broker when the client is not authorized
    to access the requested topic/broker/cluster
    """


class ConsumerCalledError(TransportException):
    """ Raises when consumer has already called """


class PublisherIllegalError(TransportException):
    """ Raises when publisher is closed forcefully. """


class NewTopicError(TransportException):
    """ Raises when created new topic, when the consumer has old messages """


class UnknownTopicError(TransportException):
    """ Raises when topic doesn't exist on this broker """


class TopicExistsError(TransportException):
    """ Raises when topic alredy exist on this broker """


class NoBrokersAvailable(TransportException):
    """ Raises when broker doesn't available """


class InitializationError(Exception):
    """Raises when init errors occur."""
