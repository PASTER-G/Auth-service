# Authentication Service - Сервис аутентификации пользователей

Этот проект демонстрирует систему аутентификации пользователей с использованием FastAPI, Redis, Kafka и PostgreSQL, развернутую с помощью Docker Compose.

## Архитектура

- **API Server**: FastAPI приложение
- **База данных**: PostgreSQL для хранения пользователей
- **Кеш**: Redis для хранения сессий
- **Message Broker**: Kafka для обработки событий
- **Контейнеризация**: Docker Compose

## Структура проекта
```
.
├── .env.example # Шаблон конфигурации
├── docker-compose.yml # Конфигурация Docker сервисов
├── Dockerfile # Docker образ приложения
├── requirements.txt # Зависимости Python
├── src/ # Исходный код приложения
│ ├── app.py # Основное FastAPI приложение
│ ├── auth.py # Логика аутентификации
│ ├── config.py # Загрузка конфигурации
│ ├── database.py # Подключение к PostgreSQL
│ ├── kafka_consumer.py # Consumer для обработки событий
│ └── kafka_producer.py # Producer для отправки событий
├── test.sh # Тестирование приложения
└── README.md
```

## Предварительные требования

- Docker >= 20.10
- Docker Compose >= 2.20

## Быстрый старт

1. Клонируйте репозиторий:
```bash
git clone https://github.com/PASTER-G/Auth-service.git
cd Auth-service
```

2. Настройте окружение:
```bash
cp .env.example .env
# Отредактируйте .env при необходимости
```
3. Запустите приложение:
```bash
docker-compose up -d
```
4. Получите доступ к сервисам: 
- **API**: http://localhost:8000
- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: http://localhost:5432
- **Redis**: http://localhost:6379
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 *(admin/admin)*

## API Эндпоинты

- *POST /register* - Регистрация пользователя
- *POST /login* - Вход в систему
- *GET /verify* - Проверка токена
- *POST /logout* - Выход из системы
- *GET /health* - Проверка здоровья сервисов

## Мониторинг

Проект включает в себя мониторинг через Prometheus и Grafana:

- **Prometheus** собирает метрики с API на эндпоинте `/metrics`
- **Grafana** визуализирует метрики с готовым дашбордом

Доступные метрики:
- Количество HTTP запросов
- Время выполнения запросов  
- Количество активных сессий
- Количество активных пользователей

## Пример использования
```bash
# Регистрация
curl -X POST "http://localhost:8000/register" \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser","password":"testpass"}'

# Логин
curl -X POST "http://localhost:8000/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser","password":"testpass"}'

# Верификация (используйте токен из ответа логина)
curl -H "Authorization: Bearer YOUR_TOKEN" "http://localhost:8000/verify"

# Логаут
curl -X POST -H "Authorization: Bearer YOUR_TOKEN" "http://localhost:8000/logout"
```

## Что можно улучшить
- Добавить миграции базы данных
- Добавить мониторинг с Prometheus и Grafana
- Настроить CI/CD пайплайн

## Автор

[PASTER-G](https://github.com/PASTER-G)   