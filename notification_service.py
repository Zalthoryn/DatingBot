import pika
import json
import logging
import os
import asyncio
import threading
from minio import Minio
from minio.error import S3Error
from aiogram.types import BufferedInputFile, InputMediaPhoto
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

minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)
if not minio_client:
    logger.error("MinIO client initialization failed: MINIO_ROOT_USER or MINIO_ROOT_PASSWORD not set")
    raise ValueError("MinIO credentials not provided")

bucket_name = "photos"

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials('ivan', 'admin1234')
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="rabbitmq",
        credentials=credentials
    ))

async def send_telegram_notification(user_info, object_keys=None):
    try:
        candidate_text = (
            f"У тебя новый мэтч!\n"
            f"Анкета:\n"
            f"Возраст: {user_info['age']}\n"
            f"Пол: {user_info['gender']}\n"
            f"Интересы: {user_info['interests']}\n"
            f"Город: {user_info['city']}."
        )
        if object_keys:
            # Создаём список медиа для отправки
            media = []
            for photo in object_keys:
                response = minio_client.get_object(bucket_name, photo)
                photo_data = response.read()
                response.close()
                response.release_conn()
                photo_file = BufferedInputFile(
                    file=photo_data,
                    filename="profile_photo.jpg"
                )
                media.append(InputMediaPhoto(media=photo_file)) # photo_file может быть file_id, URL или BufferedInputFile)
            # Отправляем все фото с текстом анкеты
            await bot.send_media_group(user_info['to_user_id'], media=media)
            await bot.send_message(user_info['to_user_id'], text=candidate_text)
        else:
            await bot.send_message(user_info['to_user_id'], text=candidate_text)
    except S3Error as e:
        logger.error(f"Error retrieving photo from MinIO: {str(e)}")
        await bot.send_message(user_info['to_user_id'], text=candidate_text)
    except Exception as e:
        logger.error(f"Failed to send notification to {user_info['to_user_id']}: {str(e)}")
        await bot.send_message(user_info['to_user_id'], text="Произошла ошибка при отправке уведомления.")

def callback(ch, method, properties, body):
    data = json.loads(body)
    user_info = data["user_info"]
    object_keys = data.get("object_keys")
    logger.info(f"Processing notification for user {user_info['to_user_id']}")
    asyncio.run_coroutine_threadsafe(send_telegram_notification(user_info, object_keys), loop)

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