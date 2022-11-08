import yaml
from os import environ
import pika
import logging
import functools

logging.getLogger("pika").setLevel(logging.WARNING)

# TODO Oban support https://github.com/surferseo/brief/pull/17#pullrequestreview-1170548768
# TODO connection retries https://github.com/surferseo/content-planner-clusterer/commit/216d5dbc20961e48f832fe81c11ddff783d5aef5


class AMQPClient:
    def __init__(
        self,
        connection,
        consumers,
        producers,
    ):
        self.connection = connection
        self.consumers = consumers
        self.producers = producers

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

    def publish(self, message):
        channel, exchange, queue, config = self.producers[0]
        self.connection.add_callback_threadsafe(
            functools.partial(
                channel.basic_publish,
                exchange=exchange,
                routing_key=queue,
                body=message,
                properties=pika.BasicProperties(
                    content_type="application/json", delivery_mode=1
                ),
            )
        )

    def publish_and_ack(self, delivery_tag, response_message):
        self.publish(response_message)
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
        exchange = config.get("exchange", "direct")
        channel.exchange_declare(exchange=exchange, durable=True)
        dlx_arguments = AMQPClient._set_dlx(channel, config, queue_prefix, queue)
        AMQPClient._queue_declare(channel, queue, config, arguments=dlx_arguments)
        channel.queue_bind(queue=queue, exchange=exchange)
        if "prefetch_count" in config:
            channel.basic_qos(prefetch_count=config["prefetch_count"])
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
            AMQPClient._from_config(consumer_config, connection, queue_prefix)
            for consumer_config in consumer_configs
        ]

        producer_configs = config["producers"]
        producers = [
            AMQPClient._from_config(producer_config, connection, queue_prefix)
            for producer_config in producer_configs
        ]

        client = AMQPClient(connection, consumers, producers)
        return client

    def get_queue_size(self):
        channel, exchange, queue, config = self.consumers[0]
        res = AMQPClient._queue_declare(channel, queue, {"passive": True})
        return res.method.message_count

    def get_prefetch_count(self):
        channel, exchange, queue, config = self.consumers[0]
        return config["prefetch_count"]
