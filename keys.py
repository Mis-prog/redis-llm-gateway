"""Единый контракт имён ключей в Redis — общий для gateway (web.py), worker
(server.py) и admin (admin.py). Здесь же единственная точка загрузки .env.

Зачем один модуль: все процессы обязаны строить имена ключей ОДИНАКОВО. Если
префикс или схема имён разойдутся между процессами, воркер начнёт забирать
«не свои» сообщения из стрима и расшифровка упадёт `InvalidToken`. Один источник
правды это исключает.

KEY_PREFIX — изоляция на ОБЩЕМ Redis (когда БД делят несколько пользователей):
  • пусто → «голые» имена (llm:requests, llm:response:…) — как было раньше,
    прод-окружение не ломается;
  • задан (напр. "mihail:") → все ключи уходят под твой префикс, и ты:
      – не читаешь и не расшифровываешь чужие payload'ы (нет InvalidToken),
      – не режешь чужие стримы своими XTRIM / XADD MAXLEN,
      – видишь в admin только своих воркеров.
  Префикс используется как есть (включи разделитель сам, напр. двоеточие).

ВАЖНО: импортируй `keys` РАНЬШЕ `crypto` — здесь грузится .env, из которого
crypto.py на уровне модуля читает GATEWAY_CRYPTO_KEY.
"""
import os

from dotenv import load_dotenv

load_dotenv()  # подхватить .env до того, как другие модули прочитают os.getenv(...)

KEY_PREFIX = os.getenv("KEY_PREFIX", "")

# Поток задач (XADD на фронте, XREADGROUP/XACK/XTRIM/XAUTOCLAIM на воркере).
# REQUEST_STREAM можно переопределить целиком; иначе собираем из префикса.
REQUEST_STREAM = os.getenv("REQUEST_STREAM") or f"{KEY_PREFIX}llm:requests"


def req_stream() -> str:
    """Поток задач — общий для фронта и воркера."""
    return REQUEST_STREAM


def resp_key(rid: str) -> str:
    """Список ответа unary-запроса: RPUSH на воркере, BLPOP на фронте."""
    return f"{KEY_PREFIX}llm:response:{rid}"


def stream_key(rid: str) -> str:
    """Поток чанков стрим-ответа: XADD на воркере, XREAD на фронте."""
    return f"{KEY_PREFIX}llm:stream:{rid}"


def stats_key(consumer: str) -> str:
    """Снапшот метрик воркера: SET на воркере, читает admin."""
    return f"{KEY_PREFIX}llm:worker:stats:{consumer}"


def stats_pattern() -> str:
    """Маска SCAN для admin — снапшоты всех воркеров своего префикса."""
    return f"{KEY_PREFIX}llm:worker:stats:*"
