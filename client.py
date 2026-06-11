"""Пример клиента к redis-llm-gateway через OpenAI SDK.

Запуск:  pip install openai && python client.py
Конфиг:  GATEWAY (по умолчанию http://localhost:8080), MODEL (qwen3-coder),
         GATEWAY_API_KEY (если на фронте включена авторизация).
"""
import json, os
from openai import OpenAI

client = OpenAI(
    base_url=f"{os.getenv('GATEWAY', 'http://localhost:8080')}/v1",
    api_key=os.getenv("GATEWAY_API_KEY", "EMPTY"),
)
MODEL = os.getenv("MODEL", "qwen3-coder")


def simple_chat():
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": "Привет, кто ты?"}],
    )
    print("chat →", resp.choices[0].message.content)


# --- function calling: полный цикл (нужен реальный vLLM с tool-парсером) ---

def get_weather(city):
    return f"в городе {city}: +18°C, ясно"        # заглушка вместо настоящего API

TOOLS = [{
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Текущая погода в городе",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    },
}]

def tool_loop():
    messages = [{"role": "user", "content": "Какая погода в Париже?"}]
    while True:
        msg = client.chat.completions.create(
            model=MODEL, messages=messages, tools=TOOLS, tool_choice="auto",
        ).choices[0].message
        messages.append(msg.model_dump(exclude_none=True))
        if not msg.tool_calls:                    # модель ответила текстом — готово
            print("tools →", msg.content)
            return
        for call in msg.tool_calls:               # модель попросила вызвать инструмент
            args = json.loads(call.function.arguments)
            messages.append({
                "role": "tool",
                "tool_call_id": call.id,
                "content": get_weather(**args),
            })


if __name__ == "__main__":
    simple_chat()
    tool_loop()
