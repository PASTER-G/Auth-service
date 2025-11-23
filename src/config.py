import os
from dotenv import load_dotenv

load_dotenv()

# Database
POSTGRES_DB = os.getenv("POSTGRES_DB", "auth_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "session-group")

# App
SECRET_KEY = os.getenv("SECRET_KEY", "fallback-secret-key")
SESSION_TTL_HOURS = int(os.getenv("SESSION_TTL_HOURS", "1"))
