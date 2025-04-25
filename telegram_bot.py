import os
import json
import pika
import logging
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, InputMediaPhoto
from minio import Minio
from minio.error import S3Error
import aiohttp
import io

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("telegram_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()

# Инициализация MinIO клиента
minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)
if not minio_client:
    logger.error("MinIO client initialization failed: MINIO_ROOT_USER or MINIO_ROOT_PASSWORD not set")
    raise ValueError("MinIO credentials not provided")

# Создаём корзину, если она не существует
# Создаём корзину, если она не существует
bucket_name = "photos"
try:
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        logger.info(f"Bucket {bucket_name} created")
except S3Error as e:
    logger.error(f"Error creating bucket: {str(e)}")

async def init_db():
    return await asyncpg.create_pool(
        user="dating_user",
        password="dating_password",
        database="dating_db",
        host="postgres"
    )

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials('ivan', 'admin1234')
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="rabbitmq",
        credentials=credentials
    ))

pool = None
user_state = {}

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    async with pool.acquire() as conn:
        # Проверяем, есть ли пользователь
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            await conn.execute(
                "INSERT INTO Users (telegram_id, username) VALUES ($1, $2)",
                user_id, message.from_user.username
            )
    await message.answer("Привет! Я бот для знакомств. Используй /profile, чтобы создать анкету!")

@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    user_id = message.from_user.id
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            await conn.execute(
                "INSERT INTO Users (telegram_id, username) VALUES ($1, $2)",
                user_id, message.from_user.username
            )
            user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)

        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user['id'])
        if profile:
            await message.answer("Твой профиль уже существует! Используй /view, чтобы посмотреть, или /addphoto, чтобы добавить фото.")
        else:
            await message.answer("Давай создадим профиль! Введи свой возраст:")
            user_state[user_id] = {"step": "age", "user_db_id": user['id']}

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "age")
async def process_age(message: types.Message):
    user_id = message.from_user.id
    user_db_id = user_state[user_id]["user_db_id"]
    try:
        age = int(message.text)
        user_state[user_id]["age"] = age
        await message.answer("Теперь укажи свой пол (м/ж):")
        user_state[user_id]["step"] = "gender"
    except ValueError:
        await message.answer("Пожалуйста, введи число для возраста!")

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "gender")
async def process_gender(message: types.Message):
    user_id = message.from_user.id
    gender = message.text.lower()
    if gender in ["м", "ж"]:
        user_state[user_id]["gender"] = gender
        await message.answer("Укажи свои интересы (через запятую):")
        user_state[user_id]["step"] = "interests"
    else:
        await message.answer("Пожалуйста, укажи пол как 'м' или 'ж'!")

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "interests")
async def process_interests(message: types.Message):
    user_id = message.from_user.id
    interests = message.text
    user_state[user_id]["interests"] = interests
    await message.answer("Укажи свой город:")
    user_state[user_id]["step"] = "city"

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "city")
async def process_city(message: types.Message):
    user_id = message.from_user.id
    user_db_id = user_state[user_id]["user_db_id"]
    city = message.text
    async with pool.acquire() as conn:
        # Создаём профиль
        await conn.execute(
            """
            INSERT INTO Profiles (user_id, age, gender, interests, city, profile_completeness)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            user_db_id, user_state[user_id]["age"], user_state[user_id]["gender"],
            user_state[user_id]["interests"], city, 80  # Начальная заполненность
        )
        # Создаём запись в Ratings
        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user_db_id)
        await conn.execute(
            "INSERT INTO Ratings (profile_id) VALUES ($1)", profile['id']
        )

    # Отправляем сообщение в очередь matchmaking
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="matchmaking")
    channel.basic_publish(
        exchange="",
        routing_key="matchmaking",
        body=json.dumps({"user_id": user_id})
    )
    connection.close()
    logger.info(f"Sent matchmaking message for user {user_id}")

    await message.answer("Профиль создан! Используй /addphoto, чтобы добавить фото, или /find, чтобы найти пару.")
    del user_state[user_id]

@dp.message(Command("addphoto"))
async def cmd_add_photo(message: types.Message):
    user_id = message.from_user.id
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            await message.answer("Сначала создай профиль с помощью /profile!")
            return
    await message.answer("Пожалуйста, отправь фото для твоего профиля:")
    user_state[user_id] = {"step": "add_photo", "user_db_id": user['id']}

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "add_photo")
async def process_photo(message: types.Message):
    user_id = message.from_user.id
    user_db_id = user_state[user_id]["user_db_id"]
    if message.photo:
        file_id = message.photo[-1].file_id
        file_info = await bot.get_file(file_id)
        file_path = file_info.file_path

        file = await bot.download_file(file_path)
        file_bytes = file.read()

        object_key = f"user{user_db_id}/photo-{file_info.file_unique_id}.jpg"
        try:
            minio_client.put_object(
                bucket_name,
                object_key,
                io.BytesIO(file_bytes),
                length=len(file_bytes),
                content_type="image/jpeg"
            )
            logger.info(f"Photo uploaded to MinIO: {object_key}")
        except S3Error as e:
            logger.error(f"Error uploading photo to MinIO: {str(e)}")
            await message.answer("Ошибка при загрузке фото. Попробуй снова!")
            del user_state[user_id]
            return

        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO Photos (user_id, object_key) VALUES ($1, $2)", user_db_id, object_key
            )
            # Обновляем profile_completeness
            await conn.execute(
                """
                UPDATE Profiles
                SET profile_completeness = LEAST(100, profile_completeness + 10)
                WHERE user_id = $1
                """,
                user_db_id
            )
        await message.answer("Фото добавлено! Используй /addphoto, чтобы добавить ещё, или /find, чтобы найти пару.")
    else:
        await message.answer("Пожалуйста, отправь фото!")
    del user_state[user_id]

@dp.message(Command("view"))
async def cmd_view(message: types.Message):
    user_id = message.from_user.id
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            await message.answer("У тебя нет профиля! Создай его с помощью /profile.")
            return
        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user['id'])
        if profile:
            profile_text = (
                f"Возраст: {profile['age']}\n"
                f"Пол: {profile['gender']}\n"
                f"Интересы: {profile['interests']}\n"
                f"Город: {profile['city']}\n"
                f"Заполненность профиля: {profile['profile_completeness']}%"
            )

            # Получаем до трёх последних фотографий
            photos = await conn.fetch(
                "SELECT object_key FROM Photos WHERE user_id = $1 ORDER BY uploaded_at DESC LIMIT 3",
                user['id']
            )

            if photos:
                try:
                    # Создаём список медиа для отправки
                    media = []
                    for photo in photos:
                        response = minio_client.get_object(bucket_name, photo['object_key'])
                        photo_data = response.read()
                        response.close()
                        response.release_conn()
                        photo_file = BufferedInputFile(
                            file=photo_data,
                            filename="profile_photo.jpg"
                        )
                        media.append(InputMediaPhoto(media=photo_file))  # photo_file может быть file_id, URL или BufferedInputFile)
                    # Отправляем все фото с текстом анкеты
                    await message.answer_media_group(media=media)
                    await message.answer(profile_text)
                except S3Error as e:
                    logger.error(f"Error retrieving photo from MinIO: {str(e)}")
                    await message.answer(profile_text)
            else:
                await message.answer(profile_text)
        else:
            await message.answer("У тебя нет профиля! Создай его с помощью /profile.")

@dp.message(Command("find"))
async def cmd_find(message: types.Message):
    user_id = message.from_user.id
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            await message.answer("Сначала создай профиль с помощью /profile!")
            return
        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user['id'])
        if not profile or profile['profile_completeness'] < 80:
            await message.answer("Пожалуйста, заполни профиль полностью с помощью /profile и добавь фото!")
            return

        # Ищем кандидата
        candidate = await conn.fetchrow(
            """
            SELECT u.telegram_id, p.id as profile_id, p.age, p.gender, p.interests, p.city
            FROM Profiles p
            JOIN Users u ON p.user_id = u.id
            WHERE u.id != $1
            AND p.gender != $2
            AND p.city = $3
            AND NOT EXISTS (
                SELECT 1 FROM Matches m
                WHERE (m.profile1_id = $4 AND m.profile2_id = p.id)
                OR (m.profile1_id = p.id AND m.profile2_id = $4)
            )
            AND NOT EXISTS (
                SELECT 1 FROM Interactions i
                WHERE i.from_profile_id = $4 AND i.to_profile_id = p.id AND i.action = 'skip'
            )
            LIMIT 1
            """,
            user['id'], profile['gender'], profile['city'], profile['id']
        )

        if not candidate:
            await message.answer("Подходящих кандидатов не найдено. Попробуй позже!")
            return

        # Получаем 1-3 фото кандидата
        photos = await conn.fetch(
            "SELECT object_key FROM Photos WHERE user_id = (SELECT id FROM Users WHERE telegram_id = $1) ORDER BY uploaded_at DESC LIMIT 3",
            candidate['telegram_id']
        )

        candidate_text = (
            f"Анкета:\n"
            f"Возраст: {candidate['age']}\n"
            f"Пол: {candidate['gender']}\n"
            f"Интересы: {candidate['interests']}\n"
            f"Город: {candidate['city']}\n"
            f"\nСогласен на мэтч? Ответь 'да' или 'нет'."
        )

        if photos:
            try:
                # Создаём список медиа для отправки
                media = []
                for photo in photos:
                    response = minio_client.get_object(bucket_name, photo['object_key'])
                    photo_data = response.read()
                    response.close()
                    response.release_conn()
                    photo_file = BufferedInputFile(
                        file=photo_data,
                        filename="profile_photo.jpg"
                    )
                    media.append(InputMediaPhoto(media=photo_file))  # photo_file может быть file_id, URL или BufferedInputFile)
                # Отправляем все фото с текстом анкеты
                await message.answer_media_group(media=media)
                await message.answer(candidate_text)
            except S3Error as e:
                logger.error(f"Error retrieving photo from MinIO: {str(e)}")
                await message.answer(candidate_text)
        else:
            await message.answer(candidate_text)

        user_state[user_id] = {
            "step": "match_response",
            "candidate_profile_id": candidate['profile_id'],
            "from_profile_id": profile['id']
        }

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "match_response")
async def process_match_response(message: types.Message):
    user_id = message.from_user.id
    response = message.text.lower()
    candidate_profile_id = user_state[user_id]["candidate_profile_id"]
    from_profile_id = user_state[user_id]["from_profile_id"]

    async with pool.acquire() as conn:
        # Сохраняем взаимодействие
        action = "like" if response == "да" else "skip"
        await conn.execute(
            """
            INSERT INTO Interactions (from_profile_id, to_profile_id, action)
            VALUES ($1, $2, $3)
            """,
            from_profile_id, candidate_profile_id, action
        )

        if response == "да":
            mutual_like = await conn.fetchrow(
                """
                SELECT 1 FROM Interactions
                WHERE from_profile_id = $1 AND to_profile_id = $2 AND action = 'like'
                """,
                candidate_profile_id, from_profile_id
            )
            if mutual_like:
                # Создаём мэтч
                await conn.execute(
                    "INSERT INTO Matches (profile1_id, profile2_id) VALUES ($1, $2)",
                    from_profile_id, candidate_profile_id
                )

                connection = get_rabbitmq_connection()
                channel = connection.channel()
                channel.queue_declare(queue="notifications")
                user1 = await conn.fetchrow(
                    """
                    SELECT u.telegram_id, p.age, p.gender, p.interests, p.city
                    FROM Users u 
                    JOIN Profiles p ON u.id = p.user_id 
                    WHERE p.id = $1
                    """,
                    from_profile_id
                )
                user2 = await conn.fetchrow(
                    """
                    SELECT u.telegram_id, p.age, p.gender, p.interests, p.city
                    FROM Users u 
                    JOIN Profiles p ON u.id = p.user_id 
                    WHERE p.id = $1
                    """,
                    candidate_profile_id
                )

                if not user1 or not user2:
                    logger.error(f"User data not found: user1={user1}, user2={user2}")
                    await message.answer("Ошибка при создании мэтча. Пожалуйста, попробуй снова.")
                    connection.close()
                    del user_state[user_id]
                    return

                # Получаем до трёх последних фотографий для каждого пользователя
                user1_photos = await conn.fetch(
                    "SELECT object_key FROM Photos WHERE user_id = (SELECT id FROM Users WHERE telegram_id = $1) ORDER BY uploaded_at DESC LIMIT 3",
                    user1['telegram_id']
                )
                user2_photos = await conn.fetch(
                    "SELECT object_key FROM Photos WHERE user_id = (SELECT id FROM Users WHERE telegram_id = $1) ORDER BY uploaded_at DESC LIMIT 3",
                    user2['telegram_id']
                )

                user1_object_keys = [photo['object_key'] for photo in user1_photos] if user1_photos else []
                user2_object_keys = [photo['object_key'] for photo in user2_photos] if user2_photos else []

                # Первому пользователю отправляем анкету второго
                channel.basic_publish(
                    exchange="",
                    routing_key="notifications",
                    body=json.dumps({
                        "user_info": {
                            "to_user_id": user1['telegram_id'],
                            "age": user2['age'],  
                            "gender": user2['gender'],
                            "interests": user2['interests'],
                            "city": user2['city']
                        },
                        "object_keys": user2_object_keys
                    })
                )
                # Второму пользователю отправляем анкету первого
                channel.basic_publish(
                    exchange="",
                    routing_key="notifications",
                    body=json.dumps({
                        "user_info": {
                            "to_user_id": user2['telegram_id'],  # Второй пользователь получает уведомление
                            "age": user1['age'],
                            "gender": user1['gender'],
                            "interests": user1['interests'],
                            "city": user1['city']
                        },
                        "object_keys": user1_object_keys
                    })
                )
                
                # Отправляем сообщение в очередь matchmaking для обоих пользователей
                channel.queue_declare(queue="matchmaking")
                channel.basic_publish(
                    exchange="",
                    routing_key="matchmaking",
                    body=json.dumps({"user_id": user1['telegram_id']})
                )
                channel.basic_publish(
                    exchange="",
                    routing_key="matchmaking",
                    body=json.dumps({"user_id": user2['telegram_id']})
                )
                logger.info(f"Sent matchmaking messages for users {user1['telegram_id']} and {user2['telegram_id']}")

                connection.close()

                await message.answer("Мэтч создан! Оба пользователя уведомлены.")
            else:
                await message.answer("Ты лайкнул этого пользователя. Ожидай, пока он тоже тебя лайкнет!")
        elif response == "нет":
            await message.answer("Пользователь пропущен. Используй /find, чтобы найти другого кандидата.")
        else:
            await message.answer("Пожалуйста, ответь 'да' или 'нет'!")
            return

    del user_state[user_id]

async def on_startup():
    global pool
    pool = await init_db()
    logger.info("Bot started with database connection")

if __name__ == "__main__":
    dp.startup.register(on_startup)
    dp.run_polling(bot)