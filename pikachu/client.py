import yaml
from os import environ
import pika
import logging
import functools

logging.getLogger("pika").setLevel(logging.WARNING)


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

    def consume(self):
        channel, exchange, queue = self.consumers[0]
        logging.info(f"[*] Waiting for messages in {queue}.")
        return channel.consume(queue=queue)

    def _ack(self, delivery_tag):
        channel, exchange, queue = self.consumers[0]
        self.connection.add_callback_threadsafe(
            functools.partial(channel.basic_ack, delivery_tag=delivery_tag)
        )

    def _publish(self, message):
        channel, exchange, queue = self.producers[0]
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
        self._publish(response_message)
        self._ack(delivery_tag)

    @staticmethod
    def _queue_declare(channel, queue, config):
        return channel.queue_declare(
            queue=queue, durable=True, passive=config.get("passive")
        )

    @staticmethod
    def _from_config(config, connection, queue_prefix):
        queue = queue_prefix + config["queue"]
        channel = connection.channel()
        exchange = config.get("exchange", "direct")
        channel.exchange_declare(exchange=exchange, durable=True)
        AMQPClient._queue_declare(channel, queue, config)
        channel.queue_bind(queue=queue, exchange=exchange)
        if "prefetch_count" in config:
            channel.basic_qos(prefetch_count=config["prefetch_count"])
        return (channel, exchange, queue)

    @staticmethod
    def from_config():
        host = environ["MESSAGING_AMQP_HOST"]
        url = f"amqp://{host}"
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
        channel, exchange, queue = self.consumers[0]
        res = AMQPClient._queue_declare(channel, queue, {"passive": True})
        return res.method.message_count
