"""In-process смоук-тест полного тракта — без живого Redis и без vLLM.

Поднимаем один общий fakeredis (web.r == server.r, как один реальный Redis в
проде), энкьюим unary- и stream-запрос ровно так, как это делает web.py (xadd
с полями id/stream и зашифрованным payload), затем прогоняем воркерный
server.process() по записям из xreadgroup и проверяем:

  • unary-ответ из BLPOP keys.resp_key(rid) расшифровывается в "эхо: <prompt>";
  • stream, собранный НАСТОЯЩИМ web._relay(), даёт тот же echo;
  • ВСЕ ключи в Redis под общим префиксом — ничего не утекло в чужой неймспейс.

Тест фиксирует два инварианта рефактора: единый неймспейс ключей (keys.py) и
отсутствие InvalidToken на нормальном тракте (один GATEWAY_CRYPTO_KEY у фронта и
воркера, crypto.py). Env для всего этого выставляет conftest.py ДО импортов ниже.
"""
import json
import asyncio

import fakeredis.aioredis

import keys
import web
import server
from crypto import encrypt, decrypt


def _body(prompt: str, stream: bool) -> dict:
    """Тело OpenAI-запроса — как его шлёт клиент во фронт."""
    return {
        "model": "smoke-model",
        "messages": [{"role": "user", "content": prompt}],
        "stream": stream,
    }


async def _enqueue(r, rid: str, body: dict, stream: bool) -> None:
    """xadd в стрим задач — копия того, что делает web.chat_completions()."""
    await r.xadd(keys.req_stream(), {
        "id": rid,                                  # маршрутные поля — в открытом виде
        "stream": "1" if stream else "0",
        "payload": encrypt(json.dumps(body)),       # тело запроса — шифртекст
    })


async def _drain_relay(rid: str) -> str:
    """Прогон настоящего web._relay(): собираем SSE-чанки обратно в текст ответа."""
    out = []
    async for sse in web._relay(rid, 0.0):
        line = sse.strip()                          # "data: {...}" либо "data: [DONE]"
        assert line.startswith("data: "), sse
        data = line[len("data: "):]
        if data == "[DONE]":
            break
        delta = json.loads(data)["choices"][0]["delta"]
        out.append(delta.get("content", ""))        # role/stop-чанки контента не несут
    return "".join(out)


async def _run() -> None:
    prompt = "привет шлюз"
    expected = f"эхо: {prompt}"

    # один общий fakeredis на фронт и воркер — как один реальный Redis между ними
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    web.r = server.r = r
    web.REPLY_TIMEOUT = 5            # _relay не должен виснуть, если "done" не пришёл (норма — данные уже в стриме)

    try:
        await server.ensure_group()                 # consumer group для xreadgroup/xack

        await _enqueue(r, "u1", _body(prompt, stream=False), stream=False)
        await _enqueue(r, "s1", _body(prompt, stream=True), stream=True)

        # воркер забирает обе задачи группой и обрабатывает каждую (echo, http не нужен)
        resp = await r.xreadgroup(server.GROUP, server.CONSUMER,
                                  {keys.req_stream(): ">"}, count=10)
        for _, entries in resp:
            for entry_id, fields in entries:
                await server.process(entry_id, fields, http=None)

        # 1) unary: BLPOP → расшифровка → "эхо: <prompt>"
        popped = await r.blpop(keys.resp_key("u1"), timeout=5)
        assert popped is not None, "воркер не положил unary-ответ"
        unary_content = json.loads(decrypt(popped[1]))["choices"][0]["message"]["content"]
        assert unary_content == expected, unary_content

        # 2) stream: настоящий web._relay() собирает тот же echo
        streamed = await _drain_relay("s1")
        assert streamed.strip() == expected, streamed
        assert streamed.strip() == unary_content    # unary и stream дают идентичный ответ

        # 3) неймспейс: все ключи под общим префиксом — нет утечки мимо KEY_PREFIX
        all_keys = await r.keys("*")
        assert all_keys, "в Redis не оказалось ни одного ключа"
        leaked = [k for k in all_keys if not k.startswith(keys.KEY_PREFIX)]
        assert not leaked, f"ключи вне префикса {keys.KEY_PREFIX!r}: {leaked}"
    finally:
        await r.aclose()


def test_gateway_roundtrip_echo():
    """Полный тракт unary+stream на fakeredis: echo, единый неймспейс, без InvalidToken."""
    asyncio.run(_run())
