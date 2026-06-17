.DEFAULT_GOAL := help

# Единая точка входа. Конфиг — в .env (см. .env.example). Детали — в
# Makefile.server и Makefile.client; здесь только простые глаголы поверх них.
# Redis и vLLM считаем уже поднятыми — их адреса задаются в .env.
-include .env
export

S := $(MAKE) --no-print-directory -f Makefile.server
C := $(MAKE) --no-print-directory -f Makefile.client

.PHONY: help init install keygen up down stop restart logs dashboard \
        worker web chat smoke ask

help: ## показать команды
	@echo "redis-llm-gateway — конфиг в .env (Redis/vLLM уже подняты)"
	@echo
	@echo "  make init        создать .env из .env.example"
	@echo "  make install     venv + зависимости (сервер и клиент)"
	@echo "  make keygen      ключ шифрования → в .env (GATEWAY_CRYPTO_KEY)"
	@echo
	@echo "  СЕРВЕР:"
	@echo "    make up        воркер + фронт в фоне"
	@echo "    make down      погасить"
	@echo "    make logs      хвост логов   ·   make dashboard  admin-панель"
	@echo
	@echo "  КЛИЕНТ (другая машина, GATEWAY в .env):"
	@echo "    make smoke     health + models + chat + stream"
	@echo "    make chat      один запрос (PROMPT=…)"
	@echo
	@echo "  детали: make -f Makefile.server help  ·  make -f Makefile.client help"

init: ## создать .env из шаблона
	@if [ -f .env ]; then echo ".env уже есть — не трогаю"; \
	 else cp .env.example .env && echo "✓ создан .env — открой и правь под себя"; fi

install: ## venv + зависимости сервера и клиента
	$(S) install
	$(C) install

keygen: ## сгенерировать GATEWAY_CRYPTO_KEY и записать в .env
	@if [ ! -f .env ]; then echo "сначала: make init"; exit 1; fi; \
	 key=$$($(S) keygen); \
	 if grep -q '^GATEWAY_CRYPTO_KEY=' .env; then \
	   tmp=$$(mktemp); sed "s|^GATEWAY_CRYPTO_KEY=.*|GATEWAY_CRYPTO_KEY=$$key|" .env > $$tmp && mv $$tmp .env; \
	 else printf 'GATEWAY_CRYPTO_KEY=%s\n' "$$key" >> .env; fi; \
	 echo "✓ GATEWAY_CRYPTO_KEY записан в .env"

# ── сервер ──
up: ## воркер + фронт в фоне
	@$(S) up
down stop: ## погасить воркер и фронт
	@$(S) stop
restart: ## перезапустить
	@$(S) restart
logs: ## хвост логов
	@$(S) logs
dashboard: ## admin-панель воркера
	@$(S) dashboard
worker: ## воркер (foreground)
	@$(S) worker
web: ## фронт (foreground)
	@$(S) web

# ── клиент ──
chat: ## один запрос (PROMPT=…)
	@$(C) chat
smoke: ## health + models + chat + stream
	@$(C) smoke
ask: ## запрос через OpenAI SDK
	@$(C) ask
