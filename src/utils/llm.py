# src/utils/llm.py
"""
Утилиты для работы с OpenAI через LangChain.
— Все комментарии и логи — на русском языке.  
— Ключ API берётся из переменной окружения OPENAI_API_KEY (загружается из .env).
"""

from __future__ import annotations

import logging
import os
from typing import Dict, List

from dotenv import load_dotenv
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

# ----------------------------- ЛОГИРОВАНИЕ -------------------------------- #
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ------------------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ------------------------ #


def _to_lc_messages(messages: List[Dict[str, str]]):
    """Преобразует список словарей в объекты LangChain Message."""
    out: List[SystemMessage | HumanMessage | AIMessage] = []
    for m in messages:
        role = m.get("role", "").lower()
        content = m.get("content", "")
        if role == "system":
            out.append(SystemMessage(content=content))
        elif role == "user":
            out.append(HumanMessage(content=content))
        elif role == "assistant":
            out.append(AIMessage(content=content))
    return out


def _build_chat(
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    **extra,
) -> ChatOpenAI:
    """Создаёт объект ChatOpenAI с заданными параметрами."""
    if not os.getenv("OPENAI_API_KEY"):
        raise EnvironmentError("В .env отсутствует OPENAI_API_KEY")
    return ChatOpenAI(
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        request_timeout=timeout,
        **extra,
    )


# ------------------------------ ОСНОВНАЯ API ------------------------------ #
async def ask_openai(
    messages: List[Dict[str, str]],
    model: str = "gpt-4.1-mini",
    max_tokens: int = 10_000,
    temperature: float = 0.5,
    timeout: int = 60,
    session_id: str = "default",
) -> str:
    """
    Асинхронно отправляет один запрос к OpenAI и возвращает полный текст ответа.

    :param messages: список сообщений вида {"role": "...", "content": "..."}
    :param model: название модели OpenAI
    :param max_tokens: лимит токенов в ответе
    :param temperature: температура генерации
    :param timeout: таймаут запроса в секундах
    :param session_id: произвольный идентификатор сессии (идёт в metadata)
    """
    try:
        chat = _build_chat(
            model,
            temperature,
            max_tokens,
            timeout,
            metadata={"session_id": session_id},
        )
        lc_messages = _to_lc_messages(messages)
        response = await chat.ainvoke(lc_messages)
        return response.content
    except Exception as exc:
        logger.error("Ошибка при запросе к OpenAI: %s", exc, exc_info=True)
        raise
