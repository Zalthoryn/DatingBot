import pika
import json
import asyncio
import asyncpg
import logging
import threading
import redis.asyncio as redis

async def init_db():
    logger.info("Initializing database pool")
    pool = await asyncpg.create_pool(
        user="dating_user",
        password="dating_password",
        database="dating_db",
        host="postgres"
    )
    logger.info("Database pool initialized successfully")
    return pool

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("matchmaking_service.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


async def cache_profiles(pool):
    async with pool.acquire() as conn:
        profiles = await conn.fetch(
            """
            SELECT telegram_id, age, gender, city, interests, combined_rating
            FROM users
            ORDER BY combined_rating DESC
            LIMIT 10
            """
        )
    for profile in profiles:
        await redis_client.setex(
            f"profile:{profile['telegram_id']}",
            3600,
            json.dumps(dict(profile))
        )

async def find_match(pool, user_id):
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT * FROM users WHERE telegram_id = $1", user_id
        )
        if not user:
            logger.info(f"User {user_id} not found in database")
            return None

        logger.info(f"Searching for match for user {user_id}, gender: {user['gender']}, city: {user['city']}")
        match = await conn.fetchrow(
            """
            SELECT telegram_id
            FROM users
            WHERE telegram_id != $1
            AND gender != $2
            AND city = $3
            AND NOT EXISTS (
                SELECT 1 FROM matches
                WHERE (user1_id = $1 AND user2_id = users.telegram_id)
                OR (user1_id = users.telegram_id AND user2_id = $1)
            )
            LIMIT 1
            """,
            user_id, user['gender'], user['city']
        )

        if match:
            logger.info(f"Match found: {match['telegram_id']}")
            await conn.execute(
                "INSERT INTO matches (user1_id, user2_id) VALUES ($1, $2)",
                user_id, match['telegram_id']
            )
            return match['telegram_id']
        logger.info("No match found")
        return None

def get_rabbitmq_connection():
    credentials = pika.PlainCredentials('ivan', 'admin1234')
    return pika.BlockingConnection(pika.ConnectionParameters(
        host="rabbitmq",
        credentials=credentials
    ))

async def calculate_ratings(pool, user_id):
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE telegram_id = $1", user_id)
        if not user:
            return

        primary = 0
        if user["age"]: primary += 1
        if user["gender"]: primary += 1
        if user["interests"]: primary += 1
        if user["city"]: primary += 1
        if user["photo_count"] > 0: primary += user["photo_count"]

        behavior = user["match_count"] * 2
        combined = primary + behavior

        await conn.execute(
            """
            UPDATE users
            SET primary_rating = $2, behavior_rating = $3, combined_rating = $4
            WHERE telegram_id = $1
            """,
            user_id, primary, behavior, combined
        )

def send_notification(user_id, match_id):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="notifications")
    channel.basic_publish(
        exchange="",
        routing_key="notifications",
        body=json.dumps({"user_id": user_id, "match_id": match_id})
    )
    connection.close()

def callback(ch, method, properties, body):
    data = json.loads(body)
    user_id = data["user_id"]
    logger.info(f"Processing match for user {user_id}")
    asyncio.run_coroutine_threadsafe(process_match(user_id), loop)
    # asyncio.ensure_future(process_match(user_id), loop=loop)

'''
async def process_match(user_id):
    match_id = await find_match(pool, user_id)
    if match_id:
        send_notification(user_id, match_id)
        send_notification(match_id, user_id)
        logger.info(f"Match found: {user_id} <-> {match_id}")
    else:
        logger.info(f"No match found for {user_id}")
'''

async def process_match(user_id):
    try:
        match_id = await find_match(pool, user_id)
        if match_id:
            logger.info(f"Sending notifications for {user_id} and {match_id}")
            send_notification(user_id, match_id)
            send_notification(match_id, user_id)
            logger.info(f"Match found: {user_id} <-> {match_id}")
        else:
            logger.info(f"No match found for {user_id}")
    except Exception as e:
        logger.error(f"Error in process_match for user {user_id}: {str(e)}")
        raise

def run_asyncio_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

if __name__ == "__main__":
    # time.sleep(10)  # Задержка для ожидания RabbitMQ и PostgreSQL
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="matchmaking")
    loop = asyncio.get_event_loop()
    pool = loop.run_until_complete(init_db())
    # Запускаем цикл событий в отдельном потоке
    threading.Thread(target=run_asyncio_loop, args=(loop,), daemon=True).start()
    channel.basic_consume(queue="matchmaking", on_message_callback=callback, auto_ack=True)
    logger.info("Matchmaking Service started...")
    channel.start_consuming()