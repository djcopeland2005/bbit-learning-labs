import pika
from producer_interface import mqProducerInterface
class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.connection = None
        self.channel = None
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # We'll first set up the connection and channel
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='Rabbitmq'))
        self.channel = self.connection.channel()
        # Declare the topic exchange
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message.encode('utf-8'),
        )

        # Close Channel
        self.channel.close()
        
        # Close Connection
        self.connection.close()