import asyncio
import logging
import os
import json
import time
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import asyncpg
import redis.asyncio as redis
import pika
import boto3
from botocore.client import Config

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("telegram_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Инициализация бота
bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()

# Глобальная переменная для пула соединений
db_pool = None

# Инициализация MinIO клиента
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='ivan',
    aws_secret_access_key='admin1234',
    config=Config(signature_version='s3v4')
)

# Создаем бакет, если не существует
try:
    s3_client.create_bucket(Bucket='avatars')
except s3_client.exceptions.BucketAlreadyOwnedByYou:
    pass

# Инициализация подключений
async def init_db():
    return await asyncpg.create_pool(
        user="dating_user",
        password="dating_password",
        database="dating_db",
        host="postgres"
    )

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

def get_rabbitmq_connection():
    return pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

# Состояния для создания анкеты
class ProfileForm(StatesGroup):
    age = State()
    gender = State()
    interests = State()
    city = State()

# Обработчик команды /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username
    logger.info(f"User {user_id} ({username}) started the bot")

    global db_pool
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (telegram_id, username) VALUES ($1, $2) ON CONFLICT (telegram_id) DO NOTHING",
            user_id, username
        )

    await message.answer("Привет! Я бот для знакомств. Создай анкету с помощью /profile, затем найди пару с /find!")

# Команда /profile для создания/редактирования анкеты
@dp.message(Command("profile"))
async def cmd_profile(message: types.Message, state: FSMContext):
    await state.set_state(ProfileForm.age)
    await message.answer("Сколько тебе лет?")

@dp.message(ProfileForm.age)
async def process_age(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Пожалуйста, введи число!")
        return
    await state.update_data(age=int(message.text))
    await state.set_state(ProfileForm.gender)
    await message.answer("Какой у тебя пол? (м/ж)")

@dp.message(ProfileForm.gender)
async def process_gender(message: types.Message, state: FSMContext):
    if message.text.lower() not in ["м", "ж"]:
        await message.answer("Пожалуйста, выбери м или ж!")
        return
    await state.update_data(gender=message.text.lower())
    await state.set_state(ProfileForm.interests)
    await message.answer("Какие у тебя интересы? (например, музыка, спорт)")

@dp.message(ProfileForm.interests)
async def process_interests(message: types.Message, state: FSMContext):
    await state.update_data(interests=message.text)
    await state.set_state(ProfileForm.city)
    await message.answer("В каком городе ты живешь?")

@dp.message(ProfileForm.city)
async def process_city(message: types.Message, state: FSMContext):
    data = await state.get_data()
    user_id = message.from_user.id

    global db_pool
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE users
            SET age = $2, gender = $3, interests = $4, city = $5
            WHERE telegram_id = $1
            """,
            user_id, data["age"], data["gender"], data["interests"], message.text
        )

    await state.clear()
    await message.answer("Анкета сохранена! Используй /view для просмотра или /find для поиска пары.")

# Команда /view для просмотра анкеты
@dp.message(Command("view"))
async def cmd_view(message: types.Message):
    user_id = message.from_user.id
    global db_pool
    async with db_pool.acquire() as conn:
        profile = await conn.fetchrow(
            "SELECT * FROM users WHERE telegram_id = $1", user_id
        )
    if profile:
        response = (
            f"Твоя анкета:\n"
            f"Возраст: {profile['age']}\n"
            f"Пол: {profile['gender']}\n"
            f"Интересы: {profile['interests']}\n"
            f"Город: {profile['city']}\n"
            f"Фото: {profile['photo_count']}"
        )
    else:
        response = "У тебя нет анкеты. Создай ее с помощью /profile!"
    await message.answer(response)

# Команда /find
@dp.message(Command("find"))
async def cmd_find(message: types.Message):
    user_id = message.from_user.id
    logger.info(f"User {user_id} initiated a match search")

    global db_pool
    async with db_pool.acquire() as conn:
        profile = await conn.fetchrow(
            "SELECT * FROM users WHERE telegram_id = $1", user_id
        )
        if not profile or profile["age"] is None:
            logger.warning(f"User {user_id} tried to find a match without a profile")
            await message.answer("Сначала заполни анкету с помощью /profile!")
            return

    if await redis_client.get(f"search:{user_id}"):
        logger.info(f"User {user_id} is on cooldown for search")
        await message.answer("Подожди немного перед новым поиском!")
        return

    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="matchmaking")
    channel.basic_publish(
        exchange="",
        routing_key="matchmaking",
        body=json.dumps({"user_id": user_id})
    )
    connection.close()

    await redis_client.setex(f"search:{user_id}", 300, "1")
    logger.info(f"Search task for user {user_id} sent to RabbitMQ")
    await message.answer("Ищу тебе пару... Ожидай уведомления!")

# Команда /upload_photo
@dp.message(Command("upload_photo"))
async def cmd_upload_photo(message: types.Message):
    await message.answer("Отправь мне фото для твоей анкеты!")

# Обработчик для фото
@dp.message(lambda message: message.content_type == types.ContentType.PHOTO)
async def handle_photo(message: types.Message):
    user_id = message.from_user.id
    photo = message.photo[-1]  # Берем фото максимального качества
    file_info = await bot.get_file(photo.file_id)
    file = await bot.download_file(file_info.file_path)

    # Загружаем в MinIO
    file_name = f"{user_id}_{photo.file_id}.jpg"
    s3_client.upload_fileobj(file, 'avatars', file_name)

    # Обновляем photo_count
    global db_pool
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET photo_count = photo_count + 1 WHERE telegram_id = $1",
            user_id
        )

    await message.answer("Фото загружено в анкету!")

# Запуск бота
async def main():
    global db_pool
    time.sleep(10)  # Ждем 10 секунд, чтобы дать другим сервисам запуститься
    db_pool = await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())