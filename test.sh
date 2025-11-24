#!/bin/bash

API_URL="http://localhost:8000"

echo "=== Тестирование API ==="

echo "1. Проверка здоровья..."
curl "$API_URL/health"
echo -e "\n"

echo "2. Регистрация пользователя..."
curl -X POST "$API_URL/register" \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser1112", "password":"testpass"}'
echo -e "\n"

echo "3. Логин пользователя..."
LOGIN_RESPONSE=$(curl -s -X POST "$API_URL/login" \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser1112", "password":"testpass"}')
echo "$LOGIN_RESPONSE"

TOKEN=$(echo "$LOGIN_RESPONSE" | grep -o '"token":"[^"]*' | cut -d'"' -f4)
echo "Токен: $TOKEN"
echo -e "\n"

echo "4. Проверка токена..."
curl -H "Authorization: Bearer $TOKEN" "$API_URL/verify"
echo -e "\n"

echo "5. Логаут..."
curl -X POST -H "Authorization: Bearer $TOKEN" "$API_URL/logout"
echo -e "\n"

echo "=== Тестирование завершено ==="