import pika
import json
import asyncio
import asyncpg
import logging
import threading
import redis.asyncio as redis
from config import RedisSettings, PostgresSettings, RabbitMQSettings # нужные переменные из config.py и .env
from celery import Celery

# Создаём экземпляры настроек
postgres_settings = PostgresSettings()
rabbitmq_settings = RabbitMQSettings()
redis_settings = RedisSettings()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("matchmaking_service.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Celery('tasks', broker=redis_settings.redis_url)

redis_client = redis.Redis(host=redis_settings.redis_host, port=redis_settings.redis_port, decode_responses=True)

async def init_db():
    logger.info("Initializing database pool")
    pool = await asyncpg.create_pool(
        user=postgres_settings.postgres_user,
        password=postgres_settings.postgres_password,
        database=postgres_settings.postgres_db,
        host=postgres_settings.postgres_host
    )
    logger.info("Database pool initialized successfully")
    return pool


def get_rabbitmq_connection():
    credentials = pika.PlainCredentials(rabbitmq_settings.rabbitmq_user, rabbitmq_settings.rabbitmq_password)
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_settings.rabbitmq_host,
        credentials=credentials
    ))

async def cache_profiles(pool):
    async with pool.acquire() as conn:
        profiles = await conn.fetch(
            """
            SELECT p.id, p.age, p.gender, p.city, p.interests, r.combined_rating
            FROM Profiles p
            JOIN Ratings r ON p.id = r.profile_id
            ORDER BY r.combined_rating DESC
            LIMIT 10
            """
        )
    for profile in profiles:
        await redis_client.setex(
            f"profile:{profile['id']}",
            3600,
            json.dumps(dict(profile))
        )

async def calculate_ratings(pool, user_id):
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            return
        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user['id'])
        if not profile:
            return

        photo_count = await conn.fetchval(
            "SELECT COUNT(*) FROM Photos WHERE user_id = $1", user['id']
        )

        match_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM Matches
            WHERE profile1_id = $1 OR profile2_id = $1
            """,
            profile['id']
        )

        primary = 0
        if profile["age"]: primary += 1
        if profile["gender"]: primary += 1
        if profile["interests"]: primary += 1
        if profile["city"]: primary += 1
        primary += photo_count

        behavior = match_count * 2
        combined = primary + behavior

        await conn.execute(
            """
            UPDATE Ratings
            SET primary_rating = $1, behavioral_rating = $2, combined_rating = $3, updated_at = NOW()
            WHERE profile_id = $4
            """,
            primary, behavior, combined, profile['id']
        )

def callback(ch, method, properties, body):
    data = json.loads(body)
    user_id = data["user_id"]
    logger.info(f"Processing user {user_id}")
    asyncio.run_coroutine_threadsafe(cache_profiles(pool), loop)
    app.send_task('tasks.calculate_ratings', args=[user_id]) # вызываем пересчет рейтинга через celery
    logger.info(f"Sent task to recalculate ratings for user {user_id}")

def run_asyncio_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

if __name__ == "__main__":
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="matchmaking")
    loop = asyncio.get_event_loop()
    pool = loop.run_until_complete(init_db())
    threading.Thread(target=run_asyncio_loop, args=(loop,), daemon=True).start()
    channel.basic_consume(queue="matchmaking", on_message_callback=callback, auto_ack=True)
    logger.info("Matchmaking Service started...")
    channel.start_consuming()