import io
import json
import pika
import logging
import asyncpg
from config import TelegramSettings, MinIOSettings, PostgresSettings, RabbitMQSettings
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton
from minio import Minio
from minio.error import S3Error
from keyboards import main_menu_keyboard, edit_profile_keyboard, remove_keyboard

# Создаём экземпляры настроек
telegram_settings = TelegramSettings()
minio_settings = MinIOSettings()
postgres_settings = PostgresSettings()
rabbitmq_settings = RabbitMQSettings()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("telegram_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

bot = Bot(token=telegram_settings.bot_token)
dp = Dispatcher()

pool = None
user_state = {}

# Инициализация MinIO клиента
minio_client = Minio(
    "minio:9000",
    access_key=minio_settings.minio_root_user,
    secret_key=minio_settings.minio_root_password,
    secure=False
)

if not minio_client:
    logger.error("MinIO client initialization failed: MINIO_ROOT_USER or MINIO_ROOT_PASSWORD not set")
    raise ValueError("MinIO credentials not provided")

bucket_name = "photos"
try:
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        logger.info(f"Bucket {bucket_name} created")
except S3Error as e:
    logger.error(f"Error creating bucket: {str(e)}")

async def init_db():
    return await asyncpg.create_pool(
        user=postgres_settings.postgres_user,
        password=postgres_settings.postgres_password,
        database=postgres_settings.postgres_db,
        host=postgres_settings.postgres_host
    )

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(rabbitmq_settings.rabbitmq_user, rabbitmq_settings.rabbitmq_password)
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_settings.rabbitmq_host,
        credentials=credentials
    ))

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            await conn.execute(
                "INSERT INTO Users (telegram_id, username) VALUES ($1, $2)",
                user_id, message.from_user.username
            )
    await message.answer(
        "Привет! Я бот для знакомств. Используй кнопки ниже для навигации.",
        reply_markup=main_menu_keyboard
    )

@dp.message(lambda message: message.text == "Поиск анкет 🔍")
async def handle_find_button(message: types.Message):
    await cmd_find(message)

@dp.message(lambda message: message.text == "Мой профиль 📝")
async def handle_view_button(message: types.Message):
    await cmd_view(message)

@dp.message(lambda message: message.text == "Редактировать ✏️")
async def handle_profile_button(message: types.Message):
    await cmd_profile(message)

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
        if not profile:
            await message.answer("Давай создадим профиль! Введи свой ник:", reply_markup=remove_keyboard)
            user_state[user_id] = {"step": "nickname", "user_db_id": user['id'], "mode": "create"}
        else:
            await message.answer("Твой профиль уже существует. Что хочешь сделать?", reply_markup=edit_profile_keyboard)
            user_state[user_id] = {"step": "profile_menu", "user_db_id": user['id']}

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "profile_menu")
async def process_profile_menu(message: types.Message):
    user_id = message.from_user.id
    choice = message.text.lower()
    if choice == "отредактировать ✏️":
        async with pool.acquire() as conn:
            profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user_state[user_id]["user_db_id"])
            user_state[user_id] = {
                "step": "nickname",
                "user_db_id": user_state[user_id]["user_db_id"],
                "mode": "edit",
                "current_nickname": profile['nickname'],
                "current_age": profile['age'],
                "current_gender": profile['gender'],
                "current_interests": profile['interests'],
                "current_city": profile['city']
            }
            skip_button = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Оставить текущее значение ⏭️", callback_data="skip_nickname")]
            ])
            await message.answer(
                f"Текущий ник: {profile['nickname']}\nВведи новый ник:",
                reply_markup=skip_button
            )
    elif choice == "назад ⬅️":
        await message.answer("Возвращаемся в главное меню.", reply_markup=main_menu_keyboard)
        del user_state[user_id]
    else:
        await message.answer("Пожалуйста, выбери 'Отредактировать ✏️' или 'Назад ⬅️'.")

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "nickname")
async def process_nickname(message: types.Message):
    user_id = message.from_user.id
    # Дополнительная проверка: если шаг изменился, игнорируем текстовый ввод
    if user_state.get(user_id, {}).get("step") != "nickname":
        return

    nickname = message.text.strip()
    if not nickname:
        await message.answer("Ник не может быть пустым! Введи свой ник:")
        return

    user_state[user_id]["nickname"] = nickname
    mode = user_state[user_id]["mode"]
    if mode == "edit":
        skip_button = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оставить текущее значение ⏭️", callback_data="skip_age")]
        ])
        await message.answer(
            f"Текущий возраст: {user_state[user_id]['current_age']}\nВведи новый возраст:",
            reply_markup=skip_button
        )
    else:
        await message.answer("Теперь введи свой возраст:", reply_markup=remove_keyboard)
    user_state[user_id]["step"] = "age"

@dp.callback_query(lambda c: c.data.startswith("skip_"))
async def process_skip_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    logger.info(f"process_skip_callback: user_id={user_id}, user_state={user_state.get(user_id, 'Not found')}")
    
    if user_id not in user_state:
        await callback_query.answer("Сессия истекла. Начни заново.")
        return

    step = callback_query.data.split("_")[1]
    if step == "nickname":
        user_state[user_id]["nickname"] = user_state[user_id]["current_nickname"]
        skip_button = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оставить текущее значение ⏭️", callback_data="skip_age")]
        ])
        await callback_query.message.answer(
            f"Текущий возраст: {user_state[user_id]['current_age']}\nВведи новый возраст:",
            reply_markup=skip_button
        )
        user_state[user_id]["step"] = "age"  # Обновляем шаг
    elif step == "age":
        user_state[user_id]["age"] = user_state[user_id]["current_age"]
        skip_button = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оставить текущее значение ⏭️", callback_data="skip_gender")]
        ])
        await callback_query.message.answer(
            f"Текущий пол: {user_state[user_id]['current_gender']}\nУкажи новый пол (м/ж):",
            reply_markup=skip_button
        )
        user_state[user_id]["step"] = "gender"  # Обновляем шаг
    elif step == "gender":
        user_state[user_id]["gender"] = user_state[user_id]["current_gender"]
        skip_button = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оставить текущее значение ⏭️", callback_data="skip_interests")]
        ])
        await callback_query.message.answer(
            f"Текущие интересы: {user_state[user_id]['current_interests']}\nУкажи новые интересы (через запятую):",
            reply_markup=skip_button
        )
        user_state[user_id]["step"] = "interests"  # Обновляем шаг
    elif step == "interests":
        user_state[user_id]["interests"] = user_state[user_id]["current_interests"]
        skip_button = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оставить текущее значение ⏭️", callback_data="skip_city")]
        ])
        await callback_query.message.answer(
            f"Текущий город: {user_state[user_id]['current_city']}\nУкажи новый город:",
            reply_markup=skip_button
        )
        user_state[user_id]["step"] = "city"  # Обновляем шаг
    elif step == "city":
        user_state[user_id]["city"] = user_state[user_id]["current_city"]
        logger.info(f"Before process_city_after_skip: user_id={user_id}, user_state={user_state[user_id]}")
        user_state[user_id]["step"] = "manage_photos"  # Обновляем шаг
        await process_city_after_skip(callback_query.message, user_id)
    elif step == "photos":
        user_state[user_id]["step"] = "manage_photos"  # Обновляем шаг
        await manage_photos(callback_query.message, user_id)

    await callback_query.answer()

async def process_city_after_skip(message: types.Message, user_id: int):  # Добавляем параметр user_id
    logger.info(f"process_city_after_skip: user_id={user_id}, user_state={user_state.get(user_id, 'Not found')}")
    
    # Проверяем, есть ли user_db_id в user_state
    if user_id not in user_state or "user_db_id" not in user_state[user_id]:
        # Если нет, запрашиваем user_db_id из базы данных
        async with pool.acquire() as connection:
            user_db_id = await connection.fetchval(
                "SELECT id FROM Users WHERE telegram_id = $1", user_id
            )
            if user_db_id is None:
                logger.error(f"User not found in database: telegram_id={user_id}")
                await message.answer("Ошибка: Пользователь не найден в базе данных. Пожалуйста, начни регистрацию заново.")
                return
            # Сохраняем user_db_id в user_state
            if user_id not in user_state:
                user_state[user_id] = {}
            user_state[user_id]["user_db_id"] = user_db_id
            logger.info(f"Restored user_db_id={user_db_id} for user_id={user_id}")
    else:
        user_db_id = user_state[user_id]["user_db_id"]

    mode = user_state[user_id]["mode"]
    city = user_state[user_id].get("city", user_state[user_id]["current_city"])
    async with pool.acquire() as conn:
        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user_db_id)
        if profile:
            await conn.execute(
                """
                UPDATE Profiles
                SET nickname = $1, age = $2, gender = $3, interests = $4, city = $5
                WHERE user_id = $6
                """,
                user_state[user_id]["nickname"], user_state[user_id]["age"], user_state[user_id]["gender"],
                user_state[user_id]["interests"], city, user_db_id
            )
            await message.answer("Профиль обновлён! Теперь давай управим твоими фото:", reply_markup=remove_keyboard)
        else:
            await conn.execute(
                """
                INSERT INTO Profiles (user_id, nickname, age, gender, interests, city, profile_completeness)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                user_db_id, user_state[user_id]["nickname"], user_state[user_id]["age"], user_state[user_id]["gender"],
                user_state[user_id]["interests"], city, 80
            )
            profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user_db_id)
            await conn.execute(
                "INSERT INTO Ratings (profile_id) VALUES ($1)", profile['id']
            )
            await message.answer("Профиль создан! Теперь давай добавим фото:", reply_markup=remove_keyboard)

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

        user_state[user_id]["step"] = "manage_photos"
        await manage_photos(message, user_id)

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "age")
async def process_age(message: types.Message):
    user_id = message.from_user.id
    if user_state.get(user_id, {}).get("step") != "age":
        return

    mode = user_state[user_id]["mode"]
    try:
        age = int(message.text)
        user_state[user_id]["age"] = age
        if mode == "edit":
            skip_button = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Оставить текущее значение ⏭️", callback_data="skip_gender")]
            ])
            await message.answer(
                f"Текущий пол: {user_state[user_id]['current_gender']}\nУкажи новый пол (м/ж):",
                reply_markup=skip_button
            )
        else:
            await message.answer("Теперь укажи свой пол (м/ж):", reply_markup=remove_keyboard)
        user_state[user_id]["step"] = "gender"
    except ValueError:
        await message.answer("Пожалуйста, введи число для возраста!")

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "gender")
async def process_gender(message: types.Message):
    user_id = message.from_user.id
    if user_state.get(user_id, {}).get("step") != "gender":
        return

    mode = user_state[user_id]["mode"]
    gender = message.text.lower()
    if gender in ["м", "ж"]:
        user_state[user_id]["gender"] = gender
        if mode == "edit":
            skip_button = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Оставить текущее значение ⏭️", callback_data="skip_interests")]
            ])
            await message.answer(
                f"Текущие интересы: {user_state[user_id]['current_interests']}\nУкажи новые интересы (через запятую):",
                reply_markup=skip_button
            )
        else:
            await message.answer("Укажи свои интересы (через запятую):", reply_markup=remove_keyboard)
        user_state[user_id]["step"] = "interests"
    else:
        await message.answer("Пожалуйста, укажи пол как 'м' или 'ж'!")

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "interests")
async def process_interests(message: types.Message):
    user_id = message.from_user.id
    if user_state.get(user_id, {}).get("step") != "interests":
        return

    mode = user_state[user_id]["mode"]
    interests = message.text
    user_state[user_id]["interests"] = interests
    if mode == "edit":
        skip_button = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Оставить текущее значение ⏭️", callback_data="skip_city")]
        ])
        await message.answer(
            f"Текущий город: {user_state[user_id]['current_city']}\nУкажи новый город:",
            reply_markup=skip_button
        )
    else:
        await message.answer("Укажи свой город:", reply_markup=remove_keyboard)
    user_state[user_id]["step"] = "city"

@dp.message(lambda message: user_state.get(message.from_user.id, {}).get("step") == "city")
async def process_city(message: types.Message):
    user_id = message.from_user.id
    if user_state.get(user_id, {}).get("step") != "city":
        return

    user_db_id = user_state[user_id]["user_db_id"]
    mode = user_state[user_id]["mode"]
    city = message.text
    user_state[user_id]["city"] = city

    async with pool.acquire() as conn:
        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user_db_id)
        if profile:
            await conn.execute(
                """
                UPDATE Profiles
                SET nickname = $1, age = $2, gender = $3, interests = $4, city = $5
                WHERE user_id = $6
                """,
                user_state[user_id]["nickname"], user_state[user_id]["age"], user_state[user_id]["gender"],
                user_state[user_id]["interests"], city, user_db_id
            )
            await message.answer("Профиль обновлён! Теперь давай управим твоими фото:", reply_markup=remove_keyboard)
        else:
            await conn.execute(
                """
                INSERT INTO Profiles (user_id, nickname, age, gender, interests, city, profile_completeness)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                user_db_id, user_state[user_id]["nickname"], user_state[user_id]["age"], user_state[user_id]["gender"],
                user_state[user_id]["interests"], city, 80
            )
            profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user_db_id)
            await conn.execute(
                "INSERT INTO Ratings (profile_id) VALUES ($1)", profile['id']
            )
            await message.answer("Профиль создан! Теперь давай добавим фото:", reply_markup=remove_keyboard)

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

        user_state[user_id]["step"] = "manage_photos"
        await manage_photos(message, user_id)

async def manage_photos(message: types.Message, user_id: int):
    if user_id not in user_state or "user_db_id" not in user_state[user_id]:
        # Если нет, запрашиваем user_db_id из базы данных
        async with pool.acquire() as connection:
            user_db_id = await connection.fetchval(
                "SELECT id FROM Users WHERE telegram_id = $1", user_id
            )
            if user_db_id is None:
                logger.error(f"User not found in database: telegram_id={user_id}")
                await message.answer("Ошибка: Пользователь не найден в базе данных. Пожалуйста, начни регистрацию заново.")
                return
            # Сохраняем user_db_id в user_state
            if user_id not in user_state:
                user_state[user_id] = {}
            user_state[user_id]["user_db_id"] = user_db_id
            logger.info(f"Restored user_db_id={user_db_id} for user_id={user_id}")
    else:
        user_db_id = user_state[user_id]["user_db_id"]

    async with pool.acquire() as conn:
        photos = await conn.fetch(
            "SELECT id, object_key FROM Photos WHERE user_id = $1 ORDER BY uploaded_at DESC LIMIT 3",
            user_db_id
        )

    if photos:
        media = []
        for photo in photos:
            try:
                response = minio_client.get_object(bucket_name, photo['object_key'])
                photo_data = response.read()
                response.close()
                response.release_conn()
                photo_file = BufferedInputFile(
                    file=photo_data,
                    filename="profile_photo.jpg"
                )
                media.append(InputMediaPhoto(media=photo_file))
            except S3Error as e:
                logger.error(f"Error retrieving photo from MinIO: {str(e)}")

        if media:
            await message.answer_media_group(media=media)

        photo_buttons = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Удалить фото #{i+1} 🗑️", callback_data=f"delete_photo_{photo['id']}")]
            for i, photo in enumerate(photos)
        ])
        photo_buttons.inline_keyboard.append([InlineKeyboardButton(text="Добавить новое фото 📸", callback_data="add_photo")])
        photo_buttons.inline_keyboard.append([InlineKeyboardButton(text="Завершить редактирование ✅", callback_data="finish_editing")])
        await message.answer("Вот твои текущие фото. Что хочешь сделать?", reply_markup=photo_buttons)
    else:
        photo_buttons = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Добавить новое фото 📸", callback_data="add_photo")],
            [InlineKeyboardButton(text="Завершить редактирование ✅", callback_data="finish_editing")]
        ])
        await message.answer("У тебя пока нет фото. Хочешь добавить?", reply_markup=photo_buttons)

@dp.callback_query(lambda c: c.data.startswith("delete_photo_"))
async def delete_photo(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in user_state:
        await callback_query.answer("Сессия истекла. Начни заново.")
        return

    photo_id = int(callback_query.data.split("_")[2])
    async with pool.acquire() as conn:
        photo = await conn.fetchrow("SELECT object_key FROM Photos WHERE id = $1", photo_id)
        if photo:
            try:
                minio_client.remove_object(bucket_name, photo['object_key'])
                await conn.execute("DELETE FROM Photos WHERE id = $1", photo_id)
                await callback_query.answer("Фото удалено!")
            except S3Error as e:
                logger.error(f"Error deleting photo from MinIO: {str(e)}")
                await callback_query.answer("Ошибка при удалении фото.")

    await manage_photos(callback_query.message, user_id)

@dp.callback_query(lambda c: c.data == "add_photo")
async def add_photo(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in user_state:
        await callback_query.answer("Сессия истекла. Начни заново.")
        return

    user_state[user_id]["step"] = "add_photo"
    await callback_query.message.answer("Пожалуйста, отправь новое фото:")
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "finish_editing")
async def finish_editing(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id not in user_state:
        await callback_query.answer("Сессия истекла. Начни заново.")
        return

    await callback_query.message.answer("Редактирование завершено!", reply_markup=main_menu_keyboard)
    del user_state[user_id]
    await callback_query.answer()

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
            user_state[user_id]["step"] = "manage_photos"
            await manage_photos(message, user_id)
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
        await message.answer("Фото добавлено!")
    else:
        await message.answer("Пожалуйста, отправь фото!")
        user_state[user_id]["step"] = "manage_photos"
        await manage_photos(message, user_id)
        return

    user_state[user_id]["step"] = "manage_photos"
    await manage_photos(message, user_id)

@dp.message(Command("view"))
async def cmd_view(message: types.Message):
    user_id = message.from_user.id
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            await message.answer("У тебя нет профиля! Создай его с помощью /profile.", reply_markup=main_menu_keyboard)
            return
        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user['id'])
        if profile:
            profile_text = (
                f"Ник: {profile['nickname']}\n"
                f"Возраст: {profile['age']}\n"
                f"Пол: {profile['gender']}\n"
                f"Интересы: {profile['interests']}\n"
                f"Город: {profile['city']}\n"
                f"Заполненность профиля: {profile['profile_completeness']}%"
            )

            photos = await conn.fetch(
                "SELECT object_key FROM Photos WHERE user_id = $1 ORDER BY uploaded_at DESC LIMIT 3",
                user['id']
            )

            if photos:
                try:
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
                        media.append(InputMediaPhoto(media=photo_file))
                    await message.answer_media_group(media=media)
                    await message.answer(profile_text, reply_markup=main_menu_keyboard)
                except S3Error as e:
                    logger.error(f"Error retrieving photo from MinIO: {str(e)}")
                    await message.answer(profile_text, reply_markup=main_menu_keyboard)
            else:
                await message.answer(profile_text, reply_markup=main_menu_keyboard)
        else:
            await message.answer("У тебя нет профиля! Создай его с помощью /profile.", reply_markup=main_menu_keyboard)

@dp.message(Command("find"))
async def cmd_find(message: types.Message):
    user_id = message.from_user.id
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            await message.answer("Сначала создай профиль с помощью /profile!", reply_markup=main_menu_keyboard)
            return
        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user['id'])
        if not profile or profile['profile_completeness'] < 80:
            await message.answer("Пожалуйста, заполни профиль полностью с помощью /profile и добавь фото!", reply_markup=main_menu_keyboard)
            return

        candidate = await conn.fetchrow(
            """
            SELECT u.telegram_id, p.id as profile_id, p.nickname, p.age, p.gender, p.interests, p.city
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
                WHERE i.from_profile_id = $4 AND i.to_profile_id = p.id
            )
            LIMIT 1
            """, #  AND i.action = 'skip'
            user['id'], profile['gender'], profile['city'], profile['id']
        )

        # Если анкета из того же города не найдена, ищем анкеты из других городов
        if not candidate:
            candidate = await conn.fetchrow(
                """
                SELECT u.telegram_id, p.id as profile_id, p.nickname, p.age, p.gender, p.interests, p.city
                FROM Profiles p
                JOIN Users u ON p.user_id = u.id
                WHERE u.id != $1
                AND p.gender != $2
                AND p.city != $3
                AND NOT EXISTS (
                    SELECT 1 FROM Matches m
                    WHERE (m.profile1_id = $4 AND m.profile2_id = p.id)
                    OR (m.profile1_id = p.id AND m.profile2_id = $4)
                )
                AND NOT EXISTS (
                    SELECT 1 FROM Interactions i
                    WHERE i.from_profile_id = $4 AND i.to_profile_id = p.id
                )
                LIMIT 1
                """,
                user['id'], profile['gender'], profile['city'], profile['id']
            )

        # Если кандидат не найден ни в одном из запросов
        if not candidate:
            await message.answer("Подходящих кандидатов не найдено. Попробуй позже!", reply_markup=main_menu_keyboard)
            return

        photos = await conn.fetch(
            "SELECT object_key FROM Photos WHERE user_id = (SELECT id FROM Users WHERE telegram_id = $1) ORDER BY uploaded_at DESC LIMIT 3",
            candidate['telegram_id']
        )

        candidate_text = (
            f"Ник: {candidate['nickname']}\n"
            f"Возраст: {candidate['age']}\n"
            f"Пол: {candidate['gender']}\n"
            f"Интересы: {candidate['interests']}\n"
            f"Город: {candidate['city']}\n"
            f"\nСогласен на мэтч? Ответь 'да' или 'нет'."
        )

        if photos:
            try:
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
                    media.append(InputMediaPhoto(media=photo_file))
                await message.answer_media_group(media=media)
                await message.answer(candidate_text, reply_markup=main_menu_keyboard)
            except S3Error as e:
                logger.error(f"Error retrieving photo from MinIO: {str(e)}")
                await message.answer(candidate_text, reply_markup=main_menu_keyboard)
        else:
            await message.answer(candidate_text, reply_markup=main_menu_keyboard)

        user_state[user_id] = {
            "step": "match_response",
            "candidate_profile_id": candidate['profile_id'],
            "from_profile_id": profile['id']
        }

async def process_match_response(message: types.Message):
    user_id = message.from_user.id
    response = message.text.lower()
    candidate_profile_id = user_state[user_id]["candidate_profile_id"]
    from_profile_id = user_state[user_id]["from_profile_id"]

    async with pool.acquire() as conn:
        # Проверка на существование взаимодействия
        existing_interaction = await conn.fetchrow(
            """
            SELECT 1 FROM Interactions
            WHERE from_profile_id = $1 AND to_profile_id = $2
            """,
            from_profile_id, candidate_profile_id
        )
        if existing_interaction:
            await message.answer("Ты уже взаимодействовал с этим пользователем!", reply_markup=main_menu_keyboard)
            del user_state[user_id]
            return

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
                await conn.execute(
                    "INSERT INTO Matches (profile1_id, profile2_id) VALUES ($1, $2)",
                    from_profile_id, candidate_profile_id
                )

                connection = get_rabbitmq_connection()
                channel = connection.channel()
                channel.queue_declare(queue="notifications")
                user1 = await conn.fetchrow(
                    """
                    SELECT u.telegram_id, p.nickname, p.age, p.gender, p.interests, p.city
                    FROM Users u 
                    JOIN Profiles p ON u.id = p.user_id 
                    WHERE p.id = $1
                    """,
                    from_profile_id
                )
                user2 = await conn.fetchrow(
                    """
                    SELECT u.telegram_id, p.nickname, p.age, p.gender, p.interests, p.city
                    FROM Users u 
                    JOIN Profiles p ON u.id = p.user_id 
                    WHERE p.id = $1
                    """,
                    candidate_profile_id
                )

                if not user1 or not user2:
                    logger.error(f"User data not found: user1={user1}, user2={user2}")
                    await message.answer("Ошибка при создании мэтча. Пожалуйста, попробуй снова.", reply_markup=main_menu_keyboard)
                    connection.close()
                    del user_state[user_id]
                    return

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

                channel.basic_publish(
                    exchange="",
                    routing_key="notifications",
                    body=json.dumps({
                        "user_info": {
                            "to_user_id": user1['telegram_id'],
                            "nickname": user2['nickname'],
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
                            "to_user_id": user2['telegram_id'],
                            "nickname": user1['nickname'],
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

                await message.answer("Мэтч создан! Оба пользователя уведомлены.", reply_markup=main_menu_keyboard)
            else:
                await message.answer("Ты лайкнул этого пользователя. Ожидай, пока он тоже тебя лайкнет!", reply_markup=main_menu_keyboard)
        elif response == "нет":
            await message.answer("Пользователь пропущен. Используй /find для поиска.", reply_markup=main_menu_keyboard)
        else:
            await message.answer("Пожалуйста, ответь 'да' или 'нет'!", reply_markup=main_menu_keyboard)
            return

    del user_state[user_id]

async def on_startup():
    global pool
    pool = await init_db()
    logger.info("Bot started with database connection")

if __name__ == "__main__":
    dp.startup.register(on_startup)
    dp.run_polling(bot)