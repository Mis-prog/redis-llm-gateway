.DEFAULT_GOAL := help

# Единая точка входа. Конфиг — в .env (см. .env.example). Детали — в
# Makefile.worker (воркер + admin) и Makefile.client (шлюз + проверки); здесь
# только простые глаголы поверх них. Redis и vLLM считаем уже поднятыми.
-include .env
export

W := $(MAKE) --no-print-directory -f Makefile.worker
C := $(MAKE) --no-print-directory -f Makefile.client

VENV ?= .venv
PY   ?= $(shell [ -x $(VENV)/bin/python ] && echo $(VENV)/bin/python || echo python3)

.PHONY: help init install keygen up down stop restart logs dashboard \
        worker admin web chat smoke ask test test-deps

help: ## показать команды
	@echo "redis-llm-gateway — конфиг в .env (Redis/vLLM уже подняты)"
	@echo
	@echo "  make init        создать .env из .env.example"
	@echo "  make install     venv + зависимости (воркер и клиент)"
	@echo "  make keygen      ключ шифрования → в .env (GATEWAY_CRYPTO_KEY)"
	@echo
	@echo "  ВОРКЕР (Makefile.worker — машина с доступом к vLLM):"
	@echo "    make worker    воркер server.py (foreground)"
	@echo "    make admin     admin-дашборд (foreground)  ·  make dashboard  открыть в браузере"
	@echo
	@echo "  ШЛЮЗ/КЛИЕНТ (Makefile.client — машина у пользователя):"
	@echo "    make web       OpenAI-фронт web.py (foreground)"
	@echo "    make chat      один запрос (PROMPT=…)  ·  make smoke  health+models+chat+stream"
	@echo
	@echo "  ЛОКАЛЬНО ВСЁ СРАЗУ:"
	@echo "    make up        воркер + admin + фронт в фоне  ·  make down  погасить  ·  make logs"
	@echo
	@echo "  ТЕСТЫ:"
	@echo "    make test      in-process смоук-тест тракта на fakeredis (без Redis/vLLM)"
	@echo
	@echo "  детали: make -f Makefile.worker help  ·  make -f Makefile.client help"

init: ## создать .env из шаблона
	@if [ -f .env ]; then echo ".env уже есть — не трогаю"; \
	 else cp .env.example .env && echo "✓ создан .env — открой и правь (обязательно задай KEY_PREFIX!)"; fi

install: ## venv + зависимости воркера и клиента
	$(W) install
	$(C) install

keygen: ## сгенерировать GATEWAY_CRYPTO_KEY и записать в .env
	@if [ ! -f .env ]; then echo "сначала: make init"; exit 1; fi; \
	 key=$$($(W) keygen); \
	 if grep -q '^GATEWAY_CRYPTO_KEY=' .env; then \
	   tmp=$$(mktemp); sed "s|^GATEWAY_CRYPTO_KEY=.*|GATEWAY_CRYPTO_KEY=$$key|" .env > $$tmp && mv $$tmp .env; \
	 else printf 'GATEWAY_CRYPTO_KEY=%s\n' "$$key" >> .env; fi; \
	 echo "✓ GATEWAY_CRYPTO_KEY записан в .env"

# ── локальный all-in-one: воркер+admin (worker) + фронт (client) ──
up: ## воркер + admin + фронт в фоне
	@$(W) up
	@$(C) up

down stop: ## погасить воркер, admin и фронт
	@$(W) stop
	@$(C) stop

restart: down up ## перезапустить весь локальный стек

logs: ## хвост логов воркера/admin/фронта
	@tail -n 40 -F /tmp/redis-llm-worker.log /tmp/redis-llm-admin.log /tmp/redis-llm-web.log 2>/dev/null

dashboard: ## открыть admin-дашборд в браузере
	@$(W) dashboard

# ── отдельные сервисы (foreground) ──
worker: ## воркер (foreground)
	@$(W) worker
admin: ## admin-дашборд (foreground)
	@$(W) admin
web: ## фронт (foreground)
	@$(C) web

# ── клиентские проверки ──
chat: ## один запрос (PROMPT=…)
	@$(C) chat
smoke: ## health + models + chat + stream
	@$(C) smoke
ask: ## запрос через OpenAI SDK
	@$(C) ask

# ── автотесты (in-process, без Redis/vLLM) ──
test-deps: ## поставить dev-зависимости теста (pytest + fakeredis) в .venv
	$(PY) -m pip install -q pytest fakeredis

test: test-deps ## смоук-тест тракта на fakeredis: unary+stream echo, неймспейс, без InvalidToken
	$(PY) -m pytest
