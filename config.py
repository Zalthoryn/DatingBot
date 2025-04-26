from pydantic_settings import BaseSettings

class BaseSettingsWithEnv(BaseSettings):
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

class TelegramSettings(BaseSettingsWithEnv):
    bot_token: str

class MinIOSettings(BaseSettingsWithEnv):
    minio_root_user: str
    minio_root_password: str

class PostgresSettings(BaseSettingsWithEnv):
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str

class RabbitMQSettings(BaseSettingsWithEnv):
    rabbitmq_user: str
    rabbitmq_password: str
    rabbitmq_host: str

class RedisSettings(BaseSettingsWithEnv):
    redis_host: str
    redis_port: int
    redis_url: str

'''
# Создаём экземпляры настроек
telegram_settings = TelegramSettings()
minio_settings = MinIOSettings()
postgres_settings = PostgresSettings()
rabbitmq_settings = RabbitMQSettings()
redis_settings = RedisSettings()
'''