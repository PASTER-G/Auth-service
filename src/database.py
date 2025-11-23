import psycopg2
import time
import logging
from contextlib import contextmanager
from src.config import POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT

logger = logging.getLogger(__name__)

def wait_for_db(timeout=30):
    """Ожидание пока база данных станет доступна"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            conn = psycopg2.connect(
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT
            )
            conn.close()
            logger.info("Database is ready!")
            return True
        except psycopg2.OperationalError as e:
            logger.info(f"Waiting for database... ({e})")
            time.sleep(2)
    
    raise Exception(f"Database not ready after {timeout} seconds")

@contextmanager
def get_db_connection():
    """Контекстный менеджер для подключения к БД"""
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    try:
        yield conn
    finally:
        conn.close()