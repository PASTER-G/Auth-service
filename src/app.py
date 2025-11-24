from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import Response
from confluent_kafka import Producer
import redis
import uuid
import json
import time
import logging

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Histogram, Gauge
from src.auth import create_user, verify_user, create_tables
from src.kafka_producer import produce_session_event
from src.config import REDIS_HOST, REDIS_PORT, SESSION_TTL_HOURS, KAFKA_BOOTSTRAP_SERVERS
from src.database import wait_for_db, get_db_connection

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'loggers': {
        'uvicorn': {
            'handlers': [],
            'propagate': False,
        },
        'uvicorn.access': {
            'handlers': [],
            'propagate': False,
        },
        'uvicorn.error': {
            'level': 'INFO',
            'propagate': True,
        },
    },
})

# Prometheus метрики
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP Requests', ['method', 'endpoint', 'status_code'])
REQUEST_LATENCY = Histogram('http_request_duration_seconds', 'HTTP request latency', ['method', 'endpoint'])
ACTIVE_SESSIONS = Gauge('active_sessions', 'Number of active sessions')
ACTIVE_USERS = Gauge('active_users', 'Number of active users')

app = FastAPI()
security = HTTPBearer()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создаем фильтр для исключения /metrics из логов
class MetricsFilter(logging.Filter):
    def filter(self, record):
        return "/metrics" not in record.getMessage()

# Применяем фильтр к логгеру uvicorn
for handler in logging.getLogger("uvicorn.access").handlers:
    handler.addFilter(MetricsFilter())

# Redis соединение
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

from pydantic import BaseModel

class LoginRequest(BaseModel):
    username: str
    password: str

class RegisterRequest(BaseModel):
    username: str
    password: str

# Middleware для сбора метрик
@app.middleware("http")
async def monitor_requests(request: Request, call_next):
    start_time = time.time()

    # Пропускаем логирование для /metrics
    if request.url.path == "/metrics":
        response = await call_next(request)
        return response

    response = await call_next(request)
    
    # Собираем метрики
    process_time = time.time() - start_time
    REQUEST_LATENCY.labels(method=request.method, endpoint=request.url.path).observe(process_time)
    REQUEST_COUNT.labels(method=request.method, endpoint=request.url.path, status_code=response.status_code).inc()
    
    logger.info(f"{request.method} {request.url.path} - {response.status_code} - {process_time:.3f}s")

    return response

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

@app.get("/metrics")
async def metrics():
    """Эндпоинт для Prometheus метрик"""
    # Обновляем метрики активных сессий и пользователей
    try:
        session_keys = redis_client.keys("session:*")
        user_keys = redis_client.keys("user:*")
        ACTIVE_SESSIONS.set(len(session_keys))
        ACTIVE_USERS.set(len(user_keys))
    except Exception as e:
        logger.error(f"Error updating metrics: {e}")
    
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

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
    
    # Храним сессии в Redis с TTL из конфига
    session_ttl_seconds = SESSION_TTL_HOURS * 3600
    redis_client.setex(f"session:{token}", session_ttl_seconds, request.username)
    redis_client.setex(f"user:{request.username}", session_ttl_seconds, token)
    
    # Отправляем в Кафку
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