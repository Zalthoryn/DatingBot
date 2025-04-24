import pika
import json
import asyncio
import asyncpg
import logging
import redis.asyncio as redis

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

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
        user = await conn.fetchrow("SELECT * FROM users WHERE telegram_id = $1", user_id)
        if not user:
            logger.warning(f"User {user_id} not found in database")
            return None

        match = None
        logger.info(f"Checking cached profiles for user {user_id}")
        for key in await redis_client.keys("profile:*"):
            cached = json.loads(await redis_client.get(key))
            if (cached["telegram_id"] != str(user_id) and
                cached["gender"] != user["gender"] and
                cached["city"] == user["city"]):
                match = int(cached["telegram_id"])
                logger.info(f"Found match in cache: {match} for user {user_id}")
                break

        if not match:
            logger.info(f"No match in cache, querying database for user {user_id}")
            await cache_profiles(pool)
            match = await conn.fetchrow(
                """
                SELECT telegram_id
                FROM users
                WHERE telegram_id != $1
                  AND gender != $2
                  AND city = $3
                ORDER BY combined_rating DESC
                LIMIT 1
                """,
                user_id, user["gender"], user["city"]
            )
            match = match["telegram_id"] if match else None

        if match:
            await conn.execute(
                "UPDATE users SET match_count = match_count + 1 WHERE telegram_id IN ($1, $2)",
                user_id, match
            )
            await calculate_ratings(pool, user_id)
            await calculate_ratings(pool, match)
            logger.info(f"Match confirmed: {user_id} <-> {match}")
    return match

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("matchmaking_service.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def init_db():
    return await asyncpg.create_pool(
        user="dating_user",
        password="dating_password",
        database="dating_db",
        host="postgres"
    )

def get_rabbitmq_connection():
    return pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))

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

async def process_match(user_id):
    pool = loop.run_until_complete(init_db())
    match_id = await find_match(pool, user_id)
    if match_id:
        send_notification(user_id, match_id)
        send_notification(match_id, user_id)
        logger.info(f"Match found: {user_id} <-> {match_id}")
    else:
        logger.info(f"No match found for {user_id}")
    await pool.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue="matchmaking")
    channel.basic_consume(queue="matchmaking", on_message_callback=callback, auto_ack=True)
    logger.info("Matchmaking Service started...")
    channel.start_consuming()