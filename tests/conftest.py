"""Окружение для смоук-теста тракта (web.py ↔ Redis ↔ server.py) на fakeredis.

Эти переменные ОБЯЗАНЫ быть выставлены ДО первого импорта keys/crypto/web/server:
все они читают os.getenv(...) на уровне модуля (KEY_PREFIX и имя стрима — в
keys.py, GATEWAY_CRYPTO_KEY — в crypto.py, VLLM_BASE_URL — в server.py).

keys.load_dotenv() ищет .env рядом с keys.py (НЕ в cwd) и грузит его с
override=False — поэтому выставленное здесь заранее имеет приоритет над любым
локальным .env. Значит echo-режим и тестовый ключ детерминированы и не зависят
от того, лежит ли рядом боевой .env с реальным GATEWAY_CRYPTO_KEY / VLLM_BASE_URL.

pytest импортирует conftest РАНЬШЕ тест-модулей, поэтому env гарантированно готов
к моменту их импорта. Сам conftest ничего из проекта не импортирует.
"""
import os

from cryptography.fernet import Fernet

os.environ["KEY_PREFIX"] = "smoke:"                                 # неймспейс — проверяем изоляцию ключей
os.environ["GATEWAY_CRYPTO_KEY"] = Fernet.generate_key().decode()  # общий ключ фронт↔воркер (нет InvalidToken)
os.environ["VLLM_BASE_URL"] = ""                                    # пусто → echo-режим воркера (без GPU)
os.environ["REQUEST_STREAM"] = ""                                  # имя стрима собираем из префикса, не из .env
