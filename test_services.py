import pika
import json
import time
from config import RabbitMQSettings

# Создаём экземпляры настроек
rabbitmq_settings = RabbitMQSettings()

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(rabbitmq_settings.rabbitmq_user, rabbitmq_settings.rabbitmq_password)
    for attempt in range(5):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(
                host=rabbitmq_settings.rabbitmq_host,
                port=5672,
                credentials=credentials
            ))
        except pika.exceptions.AMQPConnectionError:
            if attempt < 4:
                time.sleep(2)
                continue
            raise

connection = get_rabbitmq_connection()
channel = connection.channel()
channel.queue_declare(queue='matchmaking')
channel.basic_publish(
    exchange='',
    routing_key='matchmaking',
    body=json.dumps({'user_id': 123456789})
)
connection.close()