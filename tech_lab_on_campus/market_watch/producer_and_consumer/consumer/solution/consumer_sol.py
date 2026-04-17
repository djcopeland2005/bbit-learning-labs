import os
import pika
from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self,binding_key,exchange_name,queue_name):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name

        self.connection = None
        self.channel = None

        self.setupRMQConnection()
    
    def setupRMQConnection(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host = "rabbitmq")
        )

        self.channel = self.connection.channel()

        self.channel.queue_declare(
            queue = queue_name
        )

        self.channel.exchange_declare(
            exchange = exchange_name,
            exchange_type = "topic"
        )

        self.channel.queue_bind(
            exchange = exchange_name,
            queue = queue_name,
            routing_key = binding_key
        )

        self.channel.basic_consume(
            queue = queue_name,
            auto_ack = False
        )
    
    def on_message_callback(self,channel,message_frame,header_frame,body):
        self.channel.basic_ack(delivery_tag = method_frame.delivery_tag)

        print(body.decode("utf-8"))
    
    def startConsuming(self):
        print(" [*] Waiting for messages. To exit press CTRL+C")

        self.channel.start_consuming()
    
    def __del__(self):
        print("Closing RMQ connection on destruction")
        
        self.channel.close()
        self.connection.close()
        
