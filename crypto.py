"""Симметричное шифрование payload'ов между gateway (web.py) и worker (server.py).

Зачем: данные в Redis (потоки llm:requests / llm:stream и списки llm:response)
не должны читаться в открытом виде. Gateway и worker делят один ключ
GATEWAY_CRYPTO_KEY; всё, что попадает в Redis — шифртекст. Любой, у кого есть
доступ к Redis (MONITOR, дамп RDB, реплика, дамп памяти), видит только токены
Fernet и не может восстановить ни запрос пользователя, ни ответ модели.

Алгоритм: Fernet (AES-128-CBC + HMAC-SHA256) — аутентифицированное шифрование,
сам генерирует IV и подписывает шифртекст, на выходе urlsafe-base64 ascii-строка
(удобно класть в Redis, где decode_responses=True).

Ключ — urlsafe-base64, 32 байта. Сгенерировать:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

Ротация: несколько ключей через запятую в GATEWAY_CRYPTO_KEY. Шифруем всегда
первым, расшифровываем любым из списка (MultiFernet) — можно выкатить новый
ключ, дождаться истечения старых данных (RESP_TTL) и убрать старый ключ.

Если GATEWAY_CRYPTO_KEY не задан — шифрование выключено (passthrough): echo-режим
и существующие развёртывания продолжают работать без изменений. ВАЖНО: ключ
должен быть одинаковым у gateway и worker, иначе расшифровка упадёт InvalidToken.
"""
import os
import logging

log = logging.getLogger("crypto")

_RAW_KEY = os.getenv("GATEWAY_CRYPTO_KEY", "").strip()
ENABLED = bool(_RAW_KEY)

if ENABLED:
    from cryptography.fernet import Fernet, MultiFernet

    _keys = [k.strip() for k in _RAW_KEY.split(",") if k.strip()]
    _box = MultiFernet([Fernet(k) for k in _keys])

    def encrypt(text: str) -> str:
        """str -> шифртекст (ascii). Шифруем первым ключом из списка."""
        return _box.encrypt(text.encode("utf-8")).decode("ascii")

    def decrypt(token: str) -> str:
        """шифртекст -> str. Принимаем любой ключ из списка (ротация)."""
        return _box.decrypt(token.encode("ascii")).decode("utf-8")

else:
    log.warning("GATEWAY_CRYPTO_KEY не задан — payload'ы идут в Redis в ОТКРЫТОМ виде")

    def encrypt(text: str) -> str:
        return text

    def decrypt(token: str) -> str:
        return token
