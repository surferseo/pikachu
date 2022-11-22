import yaml
from os import environ
import pika
import logging
import functools

LOGGER = logging.getLogger("pika")
LOGGER.setLevel(logging.WARNING)

# TODO connection retries https://github.com/surferseo/content-planner-clusterer/commit/216d5dbc20961e48f832fe81c11ddff783d5aef5

DEFAULT_EXCHANGE = ""


class AMQPClient:
    def __init__(self, connection, consumers, producers, producers_channel):
        self.connection = connection
        self.consumers = consumers
        self.producers = producers
        self.producers_channel = producers_channel

    def teardown(self):
        for consumer in self.consumers:
            consumer["channel"].close()
        for producer in self.producers:
            producer["channel"].close()
        self._connection.close()

    def consume(self, inactivity_timeout):
        channel, exchange, queue, config = self.consumers[0]
        logging.info(f"[*] Waiting for messages in {queue}.")
        return channel.consume(queue=queue, inactivity_timeout=inactivity_timeout)

    def _ack(self, delivery_tag):
        channel, exchange, queue, config = self.consumers[0]
        self.connection.add_callback_threadsafe(
            functools.partial(channel.basic_ack, delivery_tag=delivery_tag)
        )

    def publish(self, properties, message):
        if self.producers:
            channel, exchange, queue, config = self.producers[0]
        else:
            channel = self.producers_channel
            exchange = DEFAULT_EXCHANGE
            queue = None
        routing_key = properties.headers.get("reply_to", queue)
        if not routing_key:
            raise ValueError("reply_to is not set in the message properties")
        self.connection.add_callback_threadsafe(
            functools.partial(
                channel.basic_publish,
                exchange=exchange,
                routing_key=routing_key,
                body=message,
                properties=properties,
            )
        )

    def publish_and_ack(self, delivery_tag, properties, response_message):
        self.publish(properties, response_message)
        self._ack(delivery_tag)

    def reject(self, delivery_tag, requeue=True):
        channel, exchange, queue, config = self.consumers[0]
        self.connection.add_callback_threadsafe(
            functools.partial(
                channel.basic_reject,
                delivery_tag=delivery_tag,
                requeue=requeue,
            )
        )

    def get_queue_size(self):
        channel, exchange, queue, config = self.consumers[0]
        res = AMQPClient._queue_declare(channel, queue, {"passive": True})
        return res.method.message_count

    def get_prefetch_count(self):
        channel, exchange, queue, config = self.consumers[0]
        return config["prefetch_count"]

    @staticmethod
    def _queue_declare(channel, queue, config, arguments=None):
        return channel.queue_declare(
            queue=queue,
            durable=True,
            passive=config.get("passive"),
            arguments=arguments,
        )

    @staticmethod
    def _resolve_scheme(port):
        if port == "5672":
            return "amqp"
        if port == "5671":
            return "amqps"

    @staticmethod
    def _set_dlx(channel, queue_config, queue_prefix, queue_name):
        """
        Check https://www.rabbitmq.com/dlx.html to meet the concept.
        """
        if "dlx" in queue_config:
            dlx_config = queue_config["dlx"]
            dlx_exchange = dlx_config["exchange"]
            dlx_queue = queue_prefix + dlx_config["queue"]
            arguments = {
                "x-dead-letter-exchange": dlx_exchange,
            }
            channel.exchange_declare(dlx_exchange, "direct")
            channel.queue_declare(dlx_queue)
            channel.queue_bind(dlx_queue, dlx_exchange, queue_name)
            return arguments

    @staticmethod
    def _build_url():
        scheme = AMQPClient._resolve_scheme(environ["MESSAGING_AMQP_PORT"])
        username = environ["MESSAGING_AMQP_USERNAME"]
        password = environ["MESSAGING_AMQP_PASSWORD"]
        authentication_credentials = f"{username}:{password}"
        host = environ["MESSAGING_AMQP_HOST"]
        virtual_host = environ["MESSAGING_AMQP_VIRTUAL_HOST"]

        return f"{scheme}://{authentication_credentials}@{host}{virtual_host}"

    @staticmethod
    def _from_config(config, connection, queue_prefix):
        queue = queue_prefix + config["queue"]
        channel = connection.channel()
        exchange = config.get("exchange", DEFAULT_EXCHANGE)
        dlx_arguments = AMQPClient._set_dlx(channel, config, queue_prefix, queue)
        AMQPClient._queue_declare(channel, queue, config, arguments=dlx_arguments)
        if "prefetch_count" in config:
            channel.basic_qos(prefetch_count=config["prefetch_count"])
        return (channel, exchange, queue, config)

    @staticmethod
    def _consumer_from_config(config, connection, queue_prefix):
        queue = queue_prefix + config["queue"]
        channel = connection.channel()
        exchange = config.get("exchange", DEFAULT_EXCHANGE)
        dlx_arguments = AMQPClient._set_dlx(channel, config, queue_prefix, queue)
        AMQPClient._queue_declare(channel, queue, config, arguments=dlx_arguments)
        if "prefetch_count" in config:
            channel.basic_qos(prefetch_count=config["prefetch_count"])
        return (channel, exchange, queue, config)

    @staticmethod
    def _producer_from_config(config, queue_prefix, channel):
        queue = queue_prefix + config["queue"]
        exchange = config.get("exchange", DEFAULT_EXCHANGE)
        AMQPClient._queue_declare(channel, queue, config)
        return (channel, exchange, queue, config)

    @staticmethod
    def from_config():
        url = AMQPClient._build_url()
        connection = pika.BlockingConnection(pika.URLParameters(url))

        with open("amqp-config.yml", "r") as f:
            config = yaml.safe_load(f)
        queue_prefix = environ["MESSAGING_AMQP_QUEUE_PREFIX"]

        consumer_configs = config["consumers"]
        consumers = [
            AMQPClient._consumer_from_config(consumer_config, connection, queue_prefix)
            for consumer_config in consumer_configs
        ]

        producers_channel = connection.channel()
        producer_configs = config.get("producers", [])
        producers = [
            AMQPClient._producer_from_config(
                producer_config, queue_prefix, producers_channel
            )
            for producer_config in producer_configs
        ]

        client = AMQPClient(connection, consumers, producers, producers_channel)
        return client
