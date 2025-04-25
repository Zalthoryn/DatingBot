import pika
import json
import logging
import os
import asyncio
import threading
from aiogram import Bot

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("notification_service.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

bot = Bot(token=os.getenv("BOT_TOKEN"))

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials('ivan', 'admin1234')
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="rabbitmq",
        credentials=credentials
    ))

async def send_telegram_notification(user_id, match_id):
    try:
        await bot.send_message(user_id, f"У тебя новый мэтч! Пользователь ID: {match_id}")
        logger.info(f"Notification successfully sent to user {user_id} about match with {match_id}")
    except Exception as e:
        logger.error(f"Failed to send notification to {user_id}: {str(e)}")

def callback(ch, method, properties, body):
    data = json.loads(body)
    user_id = data["user_id"]
    match_id = data["match_id"]
    logger.info(f"Processing notification for user {user_id} about match with {match_id}")
    asyncio.run_coroutine_threadsafe(send_telegram_notification(user_id, match_id), loop)

def run_asyncio_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

if __name__ == "__main__":
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="notifications")
    loop = asyncio.get_event_loop()
    threading.Thread(target=run_asyncio_loop, args=(loop,), daemon=True).start()
    channel.basic_consume(queue="notifications", on_message_callback=callback, auto_ack=True)
    logger.info("Notification Service started...")
    channel.start_consuming()