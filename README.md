# redis-llm-gateway

OpenAI-совместимый шлюз к **vLLM** поверх **Redis Streams**. Клиенты ходят по обычному
OpenAI API (`/v1/chat/completions`), запросы ставятся в очередь Redis, воркеры форвардят
их в vLLM и возвращают ответ. Поддержаны **streaming (SSE)** и **function calling** —
инструменты разбирает сам vLLM, шлюз прозрачно их пробрасывает.

```
Клиент ──HTTP /v1──▶ web.py ──Redis──▶ server.py ──HTTP──▶ vLLM
(OpenAI SDK/curl)    FastAPI  очередь   async-воркер     (+ tool-parser)
                     (шлюз)              │
                                         └──метрики──▶ Redis ◀──reads── admin.py (дашборд)
```

Шлюз (`web.py`), воркер (`server.py`) и дашборд (`admin.py`) — три независимых процесса,
их связывает только Redis. Их можно разносить по машинам (шлюз — рядом с пользователями,
воркер — рядом с GPU/vLLM), лишь бы у всех был один Redis, один `GATEWAY_CRYPTO_KEY` и
один `KEY_PREFIX`.

## Компоненты

| Файл | Что это |
|------|---------|
| `web.py` | OpenAI-фронт (FastAPI): `/v1/chat/completions`, `/v1/models`, `/health`. Точка входа, **не UI** — только JSON по HTTP. Запускается `Makefile.client`. |
| `server.py` | Async-воркер: мост Redis ↔ vLLM, держит до `MAX_INFLIGHT` запросов одновременно. Без `VLLM_BASE_URL` отвечает эхо-заглушкой. Запускается `Makefile.worker`. |
| `admin.py` | Admin-дашборд (FastAPI), отдельный процесс: читает метрики/очередь из Redis и отдаёт HTML + `/stats`. Запускается `Makefile.worker`. |
| `keys.py` | Единый контракт имён ключей в Redis (`KEY_PREFIX`) + загрузка `.env`. Общий для `web`/`server`/`admin`. |
| `crypto.py` | Симметричное шифрование payload'ов в Redis (Fernet), общее для фронта и воркера. |
| `client.py` | Пример клиента на OpenAI SDK (обычный чат + цикл function calling). |
| `Makefile.worker` / `Makefile.client` | Запуск воркерной (`server.py`+`admin.py`) и клиентской (`web.py`+проверки) частей. Зонтичный `Makefile` — обёртка над обоими. |
| `requirements.txt` | Зависимости (fastapi, uvicorn, redis, httpx, cryptography, python-dotenv). |

## Быстрый старт без GPU (эхо)

Проверить весь тракт client → web → Redis → worker без модели (Redis должен быть поднят):

```bash
make install        # venv + зависимости (воркер и клиент)
make init           # создать .env из .env.example
make keygen         # сгенерить GATEWAY_CRYPTO_KEY и записать в .env
# открой .env и задай уникальный KEY_PREFIX (напр. KEY_PREFIX=$(whoami):),
# VLLM_BASE_URL оставь пустым → воркер ответит эхо-заглушкой
make up             # воркер + admin + фронт в фоне
make chat           # → {"...","content":"эхо: Привет, кто ты?"}
make smoke          # health + models + chat + stream
make down           # погасить всё
```

## Полный запуск с vLLM

Redis и vLLM считаем уже поднятыми (их адреса — в `.env`). Каждую часть — в своём терминале
(или на своей машине):

```bash
make -f Makefile.worker worker      # воркер → vLLM (на машине с GPU/доступом к vLLM)
make -f Makefile.worker admin       # admin-дашборд (там же)
make -f Makefile.client web         # OpenAI-фронт на :8080 (рядом с пользователями)
```

vLLM, к которому ходит воркер (`VLLM_BASE_URL`), поднимается примерно так:

```bash
vllm serve Qwen/Qwen3-Coder-30B-A3B-Instruct \
  --served-model-name qwen3-coder \
  --enable-auto-tool-choice --tool-call-parser qwen3-coder \
  --port 8000
```

## Изоляция на общем Redis — `KEY_PREFIX`

Если Redis ты делишь с другими людьми, **обязательно задай уникальный `KEY_PREFIX`** в `.env`
(напр. `KEY_PREFIX=mihail:`). Тогда все ключи (`{prefix}llm:requests`, `{prefix}llm:response:…`
и т.д.) уходят под твой префикс, и ты:

- **не читаешь и не расшифровываешь чужие сообщения** — иначе воркер цепляет чужой payload из
  общего `llm:requests` и падает `InvalidToken` (это и есть типичная локальная ошибка
  «invalid token», когда в проде всё ок);
- **не режешь чужие данные** своими `XTRIM` / `XADD MAXLEN` по общему стриму;
- **видишь в admin только своих воркеров.**

Пусто (по умолчанию) → «голые» имена `llm:*`, общие для всех на этом Redis. `KEY_PREFIX` должен
**совпадать** у фронта, воркера и admin (как и `GATEWAY_CRYPTO_KEY`).

## Конфигурация — что где настраивать

Всё через переменные окружения (`make` подхватывает `.env`; прямой `python …`/`uvicorn …`
тоже — через `python-dotenv` в `keys.py`). Полный список — в [.env.example](.env.example).

### Общее (фронт + воркер + admin)

| Переменная | Default | Назначение |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | адрес Redis (один на все процессы) |
| `MODEL_NAME` | `qwen3-coder` | имя модели в API |
| `KEY_PREFIX` | — *(пусто → общие ключи)* | префикс всех ключей в Redis; **должен совпадать** у всех. См. [Изоляция](#изоляция-на-общем-redis--key_prefix) |
| `GATEWAY_CRYPTO_KEY` | — *(шифрование выкл.)* | общий ключ Fernet; **должен совпадать** у всех. См. [Шифрование](#шифрование-payloadов-в-redis) |
| `REQUEST_STREAM` | `{KEY_PREFIX}llm:requests` | поток задач (обычно не задаётся — собирается из префикса) |
| `LOG_LEVEL` | `INFO` | уровень логов |

### Воркер — `server.py`

| Переменная | Default | Назначение |
|---|---|---|
| `VLLM_BASE_URL` | — *(не задано → эхо)* | базовый URL vLLM, напр. `http://localhost:8000/v1` |
| `VLLM_API_KEY` | `EMPTY` | bearer для vLLM, если включён |
| `MAX_INFLIGHT` | `32` | сколько запросов воркер держит одновременно; ≈ `--max-num-seqs` vLLM |
| `HTTP_TIMEOUT` | `600` | таймаут запроса к vLLM, сек |
| `RESP_TTL` | `300` | TTL ключей ответа в Redis, сек |
| `CONSUMER_GROUP` | `workers` | имя consumer-group |
| `CONSUMER_NAME` | `<host>-<pid>` | имя консьюмера (уникально на процесс) |
| `IDLE_RECLAIM_MS` | `60000` | через сколько забирать «зависшие» сообщения упавших воркеров |
| `REQUEST_TTL_MS` | *(авто)* | возраст, по которому воркер режет свой стрим (`XTRIM MINID`) |
| `HEARTBEAT_SEC` | `10` | период строки-сводки в логе и публикации метрик воркера в Redis |

### Фронт — `web.py`

| Переменная | Default | Назначение |
|---|---|---|
| `GATEWAY_API_KEY` | — *(без авторизации)* | если задан — требуется `Authorization: Bearer <key>` |
| `REPLY_TIMEOUT` | `300` | сколько ждать ответ воркера, сек (иначе `504`) |
| `STREAM_MAXLEN` | `10000` | подрезка потока задач (`XADD MAXLEN ~`), чтобы не рос вечно |

Порт фронта — аргумент uvicorn (`WEB_PORT`, по умолчанию `8080`): `uvicorn web:app --port 8080`.

### Admin — `admin.py`

| Переменная | Default | Назначение |
|---|---|---|
| `ADMIN_HOST` | `127.0.0.1` | интерфейс дашборда (держи за localhost/firewall — отдаёт расшифрованный текст) |
| `ADMIN_PORT` | `8090` | порт uvicorn для `admin.py` |

**Наблюдаемость.** Воркер раз в `HEARTBEAT_SEC` пишет строку-сводку в лог
(`📊 inflight=… done=… rps=… p95=…`) и публикует свой снапшот в Redis
(`{KEY_PREFIX}llm:worker:stats:{consumer}`, с TTL). `admin.py` — отдельный FastAPI-процесс —
читает эти снапшоты, глубину очереди и consumer-группы из Redis и отдаёт дашборд: `GET /` —
живой HTML (авто-обновление; `make -f Makefile.worker dashboard` откроет в браузере),
`GET /stats` — JSON. В блоке **«Последние запросы · расшифровано»** показываются последние ~15
запросов, расшифрованные на лету (у admin есть ключ). Plaintext отдаётся **только** в ответ
`/stats` и никуда не пишется — ни в логи, ни в Redis.

### vLLM (флаги `vllm serve`)

| Флаг | Зачем |
|---|---|
| `--enable-auto-tool-choice` | включить function calling |
| `--tool-call-parser qwen3-coder` | парсер tool-calls под модель *(имя — как в твоей версии vLLM)* |
| `--served-model-name qwen3-coder` | имя модели = `MODEL_NAME` шлюза |
| `--max-num-seqs N` | потолок одновременных запросов; согласуй с `MAX_INFLIGHT` |
| `--port 8000` | должен совпадать с `VLLM_BASE_URL` |

### Redis-ключи

Все ключи начинаются с `KEY_PREFIX` (если задан):

| Ключ | Что |
|---|---|
| `{KEY_PREFIX}llm:requests` | поток задач (consumer-group `workers`) |
| `{KEY_PREFIX}llm:response:{id}` | ответ unary-запроса (list, `BLPOP`), TTL `RESP_TTL` |
| `{KEY_PREFIX}llm:stream:{id}` | чанки стрим-ответа (stream, `XREAD`), TTL `RESP_TTL` |
| `{KEY_PREFIX}llm:worker:stats:{consumer}` | метрики воркера (string JSON), TTL `3×HEARTBEAT_SEC` |

### Шифрование payload'ов в Redis

По умолчанию запросы и ответы лежат в Redis **в открытом виде** — любой с доступом
к БД (`MONITOR`, `XRANGE`, дамп RDB, реплика) читает переписку. Задай общий ключ
`GATEWAY_CRYPTO_KEY` фронту, воркеру и admin — и тело запроса (`payload`) и ответы
(`llm:response` / чанки `llm:stream`) шифруются Fernet (AES-128-CBC + HMAC). В Redis
остаётся только шифртекст; маршрутные поля `id`/`stream`/`type` — открыты (по ним
идёт роутинг). Модель угроз: **доверенные — фронт/воркер/admin, недоверенный — Redis.**

```bash
# один ключ на все процессы (make keygen запишет его в .env)
export GATEWAY_CRYPTO_KEY="$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
```

Несколько ключей через запятую → ротация (шифруем первым, расшифровываем любым). Ключ должен
совпадать у всех процессов, иначе расшифровка падает **`InvalidToken`** — та же ошибка
возникает, если воркер из общего Redis забрал чужое сообщение, поэтому задавай ещё и
`KEY_PREFIX` (см. [Изоляция](#изоляция-на-общем-redis--key_prefix)). Шифрование — между
фронтом и воркером; HTTPS до клиента это не отменяет.

## Клиент

Любой OpenAI-совместимый инструмент, `base_url = http://<host>:8080/v1`:

```python
from openai import OpenAI
c = OpenAI(base_url="http://localhost:8080/v1", api_key="EMPTY")
print(c.chat.completions.create(
    model="qwen3-coder",
    messages=[{"role": "user", "content": "Привет"}],
).choices[0].message.content)
```

Готовый пример с function calling — в `client.py` (`pip install openai && python client.py`).

**OpenHands** (через LiteLLM): `model = "openai/qwen3-coder"`, `base_url = http://<host>:8080/v1`,
`api_key = "EMPTY"`. Из docker-контейнера используй `host.docker.internal` вместо `localhost`.

## Как это работает

- **Unary**: web.py делает `XADD …llm:requests` и ждёт `BLPOP …llm:response:{id}`; воркер форвардит в vLLM и `RPUSH`-ит ответ.
- **Streaming**: воркер `XADD`-ит чанки в `…llm:stream:{id}`, web.py читает их `XREAD` и отдаёт клиенту SSE `data: …`.
- **Tools**: шлюз прозрачен — `tools`/`tool_choice` уходят в vLLM, `tool_calls` возвращаются клиенту; клиент исполняет инструмент и шлёт результат (`role:"tool"`) следующим запросом. Цикл повторяется, пока в ответе есть `tool_calls`.

## Эксплуатация

- **Масштаб**: один async-воркер тянет `MAX_INFLIGHT` запросов; для большего — запускай несколько `server.py` (consumer-group балансирует) и/или несколько vLLM.
- **Надёжность**: at-least-once — `xack` после ответа, зависшие сообщения упавших воркеров забираются через `xautoclaim` на старте.
- **Порядок** ответов не гарантирован (каждый адресуется по своему `request_id`).
- **Не реализовано**: `/v1/embeddings`, `/v1/completions` (legacy) — при необходимости добавляются тем же прозрачным проксированием.
