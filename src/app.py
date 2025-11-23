from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from confluent_kafka import Producer
import redis
import uuid
import json
import time
import logging

from src.auth import create_user, verify_user, create_tables
from src.kafka_producer import produce_session_event
from src.config import REDIS_HOST, REDIS_PORT, SESSION_TTL_HOURS, KAFKA_BOOTSTRAP_SERVERS
from src.database import wait_for_db, get_db_connection

app = FastAPI()
security = HTTPBearer()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis connection
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

from pydantic import BaseModel

class LoginRequest(BaseModel):
    username: str
    password: str

class RegisterRequest(BaseModel):
    username: str
    password: str

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    username = redis_client.get(f"session:{token}")
    if not username:
        raise HTTPException(status_code=401, detail="Invalid token")
    return username

@app.on_event("startup")
async def startup_event():
    """Автоматическая инициализация при запуске приложения"""
    logger.info("Starting application initialization...")
    
    # Ждем пока база данных станет доступна
    logger.info("Waiting for database to be ready...")
    wait_for_db()
    
    # Инициализируем базу данных
    logger.info("Initializing database...")
    create_tables()
    
    # Создаем тестового пользователя, если его нет
    try:
        create_user("testuser", "testpassword")
        logger.info("Created test user: testuser / testpassword")
    except Exception as e:
        logger.info("Test user already exists or couldn't be created")
    
    logger.info("Application initialization completed!")

@app.post("/register")
async def register(request: RegisterRequest):
    try:
        create_user(request.username, request.password)
        return {"status": "user created"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=e)

@app.post("/login")
async def login(request: LoginRequest):
    if not verify_user(request.username, request.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = str(uuid.uuid4())
    
    # Store session in Redis with TTL from config
    session_ttl_seconds = SESSION_TTL_HOURS * 3600
    redis_client.setex(f"session:{token}", session_ttl_seconds, request.username)
    redis_client.setex(f"user:{request.username}", session_ttl_seconds, token)
    
    # Send to Kafka
    produce_session_event("login", request.username, token)
    
    return {"token": token}

@app.get("/verify")
async def verify(user: str = Depends(get_current_user)):
    produce_session_event("verify", user)
    return {"username": user}

@app.post("/logout")
async def logout(user: str = Depends(get_current_user)):
    token = redis_client.get(f"user:{user}")
    if token:
        redis_client.delete(f"session:{token}")
        redis_client.delete(f"user:{user}")
        produce_session_event("logout", user, token)
    
    return {"status": "logged out"}

@app.get("/health")
async def health():
    """Health check endpoint"""
    try:
        # Проверяем соединение с Redis
        redis_client.ping()
        
        # Проверяем соединение с PostgreSQL
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        
        # Проверяем соединение с Kafka
        from confluent_kafka import Producer
        from src.config import KAFKA_BOOTSTRAP_SERVERS
        
        kafka_producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'socket.timeout.ms': 2000,  # 2 секунды таймаут
            'message.timeout.ms': 2000
        })
        
        kafka_producer.list_topics(timeout=2)
        kafka_producer.flush(timeout=1)
        
        return {
            "status": "healthy",
            "redis": "connected",
            "postgres": "connected",
            "kafka": "connected"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

# @app.get("/config")
# async def config():
#     """Эндпоинт для проверки конфигурации (только для разработки)"""
#     from src.config import POSTGRES_HOST, REDIS_HOST, KAFKA_BOOTSTRAP_SERVERS, SESSION_TTL_HOURS
#     return {
#         "postgres_host": POSTGRES_HOST,
#         "redis_host": REDIS_HOST,
#         "kafka_servers": KAFKA_BOOTSTRAP_SERVERS,
#         "session_ttl_hours": SESSION_TTL_HOURS
#     }