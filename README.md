# redis-llm-gateway

OpenAI-совместимый шлюз к **vLLM** поверх **Redis Streams**. Клиенты ходят по обычному
OpenAI API (`/v1/chat/completions`), запросы ставятся в очередь Redis, воркеры форвардят
их в vLLM и возвращают ответ. Поддержаны **streaming (SSE)** и **function calling** —
инструменты разбирает сам vLLM, шлюз прозрачно их пробрасывает.

```
Клиент ──HTTP /v1──▶ web.py ──Redis──▶ server.py ──HTTP──▶ vLLM
(OpenAI SDK/curl)    FastAPI  очередь   async-воркер     (+ tool-parser)
```

## Компоненты

| Файл | Что это |
|------|---------|
| `web.py` | OpenAI-фронт (FastAPI): `/v1/chat/completions`, `/v1/models`, `/health`. Точка входа, **не UI** — только JSON по HTTP. |
| `server.py` | Async-воркер: мост Redis ↔ vLLM, держит до `MAX_INFLIGHT` запросов одновременно. Без `VLLM_BASE_URL` отвечает эхо-заглушкой. |
| `client.py` | Пример клиента на OpenAI SDK (обычный чат + цикл function calling). |
| `Makefile.server` / `Makefile.client` | Запуск серверной и клиентской частей. |
| `requirements.txt` | Зависимости серверной части (fastapi, uvicorn, redis, httpx). |

## Быстрый старт без GPU (эхо)

Проверить весь тракт client → web → Redis → worker без модели:

```bash
pip install -r requirements.txt    # лучше в venv: python3 -m venv .venv && source .venv/bin/activate
make -f Makefile.server dev         # redis (docker) + воркер-эхо + фронт, в фоне
make -f Makefile.client chat        # → {"...","content":"эхо: Привет, кто ты?"}
make -f Makefile.server stop        # погасить всё
```

## Полный запуск с vLLM

Каждую цель — в своём терминале:

```bash
make -f Makefile.server redis       # 1. Redis (или свой redis-server)
make -f Makefile.server vllm        # 2. vLLM (нужен GPU)
make -f Makefile.server worker      # 3. воркер → vLLM
make -f Makefile.server web         # 4. OpenAI-фронт на :8080
```

Цель `vllm` поднимает примерно это:

```bash
vllm serve Qwen/Qwen3-Coder-30B-A3B-Instruct \
  --served-model-name qwen3-coder \
  --enable-auto-tool-choice --tool-call-parser qwen3-coder \
  --port 8000
```

## Конфигурация — что где настраивать

Всё через переменные окружения.

### Воркер — `server.py`

| Переменная | Default | Назначение |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | адрес Redis |
| `VLLM_BASE_URL` | — *(не задано → эхо)* | базовый URL vLLM, напр. `http://localhost:8000/v1` |
| `VLLM_API_KEY` | `EMPTY` | bearer для vLLM, если включён |
| `MODEL_NAME` | `qwen3-coder` | имя модели по умолчанию, если клиент не прислал |
| `MAX_INFLIGHT` | `32` | сколько запросов воркер держит одновременно; ≈ `--max-num-seqs` vLLM |
| `HTTP_TIMEOUT` | `600` | таймаут запроса к vLLM, сек |
| `RESP_TTL` | `300` | TTL ключей ответа в Redis, сек |
| `CONSUMER_GROUP` | `workers` | имя consumer-group |
| `CONSUMER_NAME` | `<host>-<pid>` | имя консьюмера (уникально на процесс) |
| `REQUEST_STREAM` | `llm:requests` | поток задач |
| `IDLE_RECLAIM_MS` | `60000` | через сколько забирать «зависшие» сообщения упавших воркеров |

### Фронт — `web.py`

| Переменная | Default | Назначение |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | адрес Redis (тот же, что у воркера) |
| `MODEL_NAME` | `qwen3-coder` | что отдавать в `/v1/models` |
| `GATEWAY_API_KEY` | — *(без авторизации)* | если задан — требуется `Authorization: Bearer <key>` |
| `REPLY_TIMEOUT` | `300` | сколько ждать ответ воркера, сек (иначе `504`) |
| `REQUEST_STREAM` | `llm:requests` | поток задач (тот же, что у воркера) |

Порт фронта — аргумент uvicorn: `uvicorn web:app --host 0.0.0.0 --port 8080`.

### vLLM (флаги `vllm serve`)

| Флаг | Зачем |
|---|---|
| `--enable-auto-tool-choice` | включить function calling |
| `--tool-call-parser qwen3-coder` | парсер tool-calls под модель *(имя — как в твоей версии vLLM)* |
| `--served-model-name qwen3-coder` | имя модели = `MODEL_NAME` шлюза |
| `--max-num-seqs N` | потолок одновременных запросов; согласуй с `MAX_INFLIGHT` |
| `--port 8000` | должен совпадать с `VLLM_BASE_URL` |

### Переменные Makefile

- `Makefile.server`: `MODEL`, `MODEL_NAME`, `VLLM_PORT`, `WEB_PORT`, `MAX_INFLIGHT`, `REDIS_URL`, `PY`
- `Makefile.client`: `GATEWAY`, `MODEL`, `PROMPT`, `PY`

Переопределяй прямо в команде: `make -f Makefile.server worker MAX_INFLIGHT=64`.

### Redis-ключи

| Ключ | Что |
|---|---|
| `llm:requests` | поток задач (consumer-group `workers`) |
| `llm:response:{id}` | ответ unary-запроса (list, `BLPOP`), TTL `RESP_TTL` |
| `llm:stream:{id}` | чанки стрим-ответа (stream, `XREAD`), TTL `RESP_TTL` |

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

- **Unary**: web.py делает `XADD llm:requests` и ждёт `BLPOP llm:response:{id}`; воркер форвардит в vLLM и `RPUSH`-ит ответ.
- **Streaming**: воркер `XADD`-ит чанки в `llm:stream:{id}`, web.py читает их `XREAD` и отдаёт клиенту SSE `data: …`.
- **Tools**: шлюз прозрачен — `tools`/`tool_choice` уходят в vLLM, `tool_calls` возвращаются клиенту; клиент исполняет инструмент и шлёт результат (`role:"tool"`) следующим запросом. Цикл повторяется, пока в ответе есть `tool_calls`.

## Эксплуатация

- **Масштаб**: один async-воркер тянет `MAX_INFLIGHT` запросов; для большего — запускай несколько `server.py` (consumer-group балансирует) и/или несколько vLLM.
- **Надёжность**: at-least-once — `xack` после ответа, зависшие сообщения упавших воркеров забираются через `xautoclaim` на старте.
- **Порядок** ответов не гарантирован (каждый адресуется по своему `request_id`).
- **Не реализовано**: `/v1/embeddings`, `/v1/completions` (legacy) — при необходимости добавляются тем же прозрачным проксированием.
