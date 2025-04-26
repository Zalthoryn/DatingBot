import asyncio
import asyncpg
import logging
from config import RedisSettings, PostgresSettings
from celery import Celery

# Создаём экземпляры настроек
postgres_settings = PostgresSettings()
redis_settings = RedisSettings()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("tasks.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Celery('tasks', broker=redis_settings.redis_url)
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)
app.config_from_object('celeryconfig')

async def init_db():
    return await asyncpg.create_pool(
        user=postgres_settings.postgres_user,
        password=postgres_settings.postgres_password,
        database=postgres_settings.postgres_db,
        host=postgres_settings.postgres_host
    )

@app.task
async def calculate_ratings(user_id):
    pool = await init_db()
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM Users WHERE telegram_id = $1", user_id)
        if not user:
            logger.warning(f"User with telegram_id {user_id} not found")
            return

        profile = await conn.fetchrow("SELECT * FROM Profiles WHERE user_id = $1", user['id'])
        if not profile:
            logger.warning(f"Profile for user {user_id} not found")
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
        primary += min(1, photo_count)

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
        logger.info(f"Ratings updated for user {user_id}: primary={primary}, behavior={behavior}, combined={combined}")
    await pool.close()

@app.task
def recalculate_ratings():
    loop = asyncio.get_event_loop()
    pool = loop.run_until_complete(init_db())
    async def run():
        users = await pool.fetch("SELECT telegram_id FROM Users")
        for user in users:
            await calculate_ratings(user["telegram_id"])
        await pool.close()
    loop.run_until_complete(run())
    logger.info("Ratings recalculated")