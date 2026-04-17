import pika
from producer_interface import mqProducerInterface
class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.Connection = None
        self.Channel = None
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # We'll first set up the connection and channel
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='Rabbitmq'))
        channel = connection.channel()

        # Declare the topic exchange
        channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message.encode('UTF-8'),
        )

        # Close Channel
        channel.close()
        
        # Close Connection
        connection.close()