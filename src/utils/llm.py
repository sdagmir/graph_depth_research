# src/utils/llm.py
"""
Утилиты для обращения к OpenAI через LangChain.
Все сообщения логируются на русском. Ключ берётся из .env (OPENAI_API_KEY).
"""

import os
import logging
from typing import List, Dict

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------------- #
#                    ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ                                  #
# --------------------------------------------------------------------------- #


def _to_lc_messages(messages: List[Dict[str, str]]):
    """Преобразуем [{role, content}, …] → LangChain-сообщения."""
    result = []
    for m in messages:
        role = m.get("role", "").lower()
        content = m.get("content", "")
        if role == "system":
            result.append(SystemMessage(content=content))
        elif role == "user":
            result.append(HumanMessage(content=content))
        elif role == "assistant":
            result.append(AIMessage(content=content))
    return result


def _build_chat(model: str, temperature: float, max_tokens: int, timeout: int, **extra):
    """Создаём объект ChatOpenAI с параметрами и проверяем ключ."""
    if not os.getenv("OPENAI_API_KEY"):
        raise EnvironmentError("В .env отсутствует OPENAI_API_KEY")
    return ChatOpenAI(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        request_timeout=timeout,
        **extra,
    )


# --------------------------------------------------------------------------- #
#                       АСИНХРОННЫЙ ОДИНОЧНЫЙ ЗАПРОС                          #
# --------------------------------------------------------------------------- #
async def ask_openai(
    messages: List[Dict[str, str]],
    model: str = "gpt-4.1-mini",
    max_tokens: int = 10000,
    temperature: float = 0.5,
    timeout: int = 60,
    session_id: str = "default",
) -> str:
    """
    Асинхронный единичный запрос к OpenAI.
    Возвращает полный ответ модели.
    """
    try:
        chat = _build_chat(model, temperature, max_tokens,
                           timeout, metadata={"session_id": session_id})
        lc_messages = _to_lc_messages(messages)
        response = await chat.ainvoke(lc_messages)
        return response.content
    except Exception as exc:
        logger.error("Ошибка при запросе к OpenAI: %s", exc, exc_info=True)
        raise
