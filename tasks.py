from celery import Celery
import asyncio
import asyncpg
import logging

app = Celery('tasks', broker='redis://redis:6379/0')
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)
app.config_from_object('celeryconfig')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def init_db():
    return await asyncpg.create_pool(
        user="dating_user",
        password="dating_password",
        database="dating_db",
        host="postgres"
    )

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

@app.task
def recalculate_ratings():
    loop = asyncio.get_event_loop()
    pool = loop.run_until_complete(init_db())
    async def run():
        users = await pool.fetch("SELECT telegram_id FROM users")
        for user in users:
            await calculate_ratings(pool, user["telegram_id"])
        await pool.close()
    loop.run_until_complete(run())
    logger.info("Ratings recalculated")