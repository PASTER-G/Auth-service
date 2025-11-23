from passlib.context import CryptContext
from src.database import get_db_connection

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def create_tables():
    """Создание необходимых таблиц в базе данных"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

def create_user(username: str, password: str):
    """Создание нового пользователя с хешированием пароля"""
    password_hash = pwd_context.hash(password)
    with get_db_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                (username, password_hash)
            )
            conn.commit()
        except Exception as e:
            # Если пользователь уже существует, пробрасываем исключение
            if "unique constraint" in str(e).lower():
                raise Exception("Username already exists")
            raise e

def verify_user(username: str, password: str) -> bool:
    """Проверка логина и пароля пользователя"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT password_hash FROM users WHERE username = %s",
            (username,)
        )
        result = cur.fetchone()
        if result:
            return pwd_context.verify(password, result[0])
        return False

def user_exists(username: str) -> bool:
    """Проверка существования пользователя"""
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM users WHERE username = %s",
            (username,)
        )
        return cur.fetchone() is not None