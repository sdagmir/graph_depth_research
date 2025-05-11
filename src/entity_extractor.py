# src/entity_extractor.py

from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import textdistance
from dotenv import load_dotenv
from tqdm import tqdm

from config import ConfigLoader
from utils.llm import ask_openai

# --------------------------- ЛОГИРОВАНИЕ ---------------------------------- #
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class EntityExtractor:
    """Запрашивает LLM и формирует doc_entities.json."""

    # ------------------------- КОНСТАНТЫ ----------------------------------- #
    MAX_CHUNK_SIZE = 8_000
    DUP_THRESHOLD = 0.85
    STOPWORDS = {
        "App", "Application", "Data", "File", "System",
        "Приложение", "Данные", "Система", "Файл",
    }

    # ----------------------- ИНИЦИАЛИЗАЦИЯ --------------------------------- #
    def __init__(self, config: ConfigLoader) -> None:
        self.config = config
        self.corpus_dir = self.config.paths["processed_corpus"]

        project_root = Path(__file__).resolve().parent.parent
        self.output_path = (
            project_root / "data/processed/doc_entities.json").resolve()

        self.llm_params: Dict = self.config["llm"]
        self._check_dirs()

    # ----------------- ПРОВЕРКА НАЛИЧИЯ КОРПУСА ---------------------------- #
    def _check_dirs(self) -> None:
        if not self.corpus_dir.exists():
            raise FileNotFoundError(
                f"Каталог корпуса не найден: {self.corpus_dir}")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    # --------------------- ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ -------------------------- #
    @staticmethod
    def _to_camel_case(text: str) -> str:
        text = re.sub(r"[^\w\s]", "", text)
        return "".join(w.capitalize() for w in text.split())

    def _deduplicate(self, ents: List[str]) -> List[str]:
        uniq: List[str] = []
        for ent in ents:
            ent = ent.strip()
            if len(ent) <= 2 or ent in self.STOPWORDS:
                continue
            if not any(
                textdistance.jaro_winkler(
                    ent.lower(), u.lower()) > self.DUP_THRESHOLD
                for u in uniq
            ):
                uniq.append(ent)
        return sorted(uniq)

    # --------------------- ЗАПРОС К LLM (АСИНХРОННО) ----------------------- #
    async def _fetch_async(self, chunk: str) -> List[str]:
        """Отправляет кусок текста в LLM и возвращает список сущностей."""
        messages = [
            {
                "role": "system",
                "content": self.llm_params.get(
                    "system_prompt",
                    "Извлеки специализированные термины из текста. "
                    "Ответ верни СТРОГО JSON-массивом без комментариев.",
                ),
            },
            {"role": "user", "content": chunk},
        ]
        try:
            raw = await ask_openai(
                messages,
                model=self.llm_params.get("model", "gpt-4o-mini"),
                max_tokens=self.llm_params.get("max_tokens", 800),
                temperature=self.llm_params.get("temperature", 0.0),
            )
            match = re.search(r"\[.*\]", raw, flags=re.DOTALL)
            return json.loads(match.group()) if match else []
        except Exception as exc:
            logger.error("Ошибка LLM: %s", exc)
            return []

    def _call_llm(self, chunk: str) -> List[str]:
        """Синхронная обёртка для удобного вызова в цикле."""
        return asyncio.run(self._fetch_async(chunk))

    # ------------------ ОБРАБОТКА ОДНОГО ФАЙЛА ----------------------------- #
    def _process_file(self, path: Path) -> Optional[Dict[str, List[str]]]:
        try:
            content = path.read_text(encoding="utf-8")
            m = re.search(r"doc_id:\s*(\d+)", content)
            if not m:
                logger.warning("В файле %s отсутствует doc_id.", path.name)
                return None

            doc_id: str = m.group(1)
            body = content.split("\n", 1)[1]

            chunks = [body[i: i + self.MAX_CHUNK_SIZE]
                      for i in range(0, len(body), self.MAX_CHUNK_SIZE)]

            raw: List[str] = []
            for chunk in chunks:
                raw.extend(self._call_llm(chunk))

            cleaned = self._deduplicate([self._to_camel_case(e) for e in raw])
            return {doc_id: cleaned} if cleaned else None

        except Exception as exc:
            logger.error("Ошибка при обработке %s: %s", path.name, exc)
            return None

    # ------------------------- ОСНОВНОЙ ПАЙПЛАЙН --------------------------- #
    def run(self) -> None:
        txt_files = sorted(self.corpus_dir.glob("*.txt"))
        if not txt_files:
            logger.warning(
                "В корпусе %s не найдено *.txt файлов.", self.corpus_dir)
            return

        result: Dict[str, List[str]] = {}
        for file in tqdm(txt_files, desc="Извлечение сущностей"):
            res = self._process_file(file)
            if res:
                result.update(res)

        if not result:
            logger.warning("Не удалось извлечь ни одной сущности.")
            return

        tmp = self.output_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(result, ensure_ascii=False,
                       indent=2), encoding="utf-8")
        tmp.replace(self.output_path)
        logger.info("Сущности сохранены в %s", self.output_path.resolve())


# ----------------------------- ЗАПУСК -------------------------------------- #
if __name__ == "__main__":
    cfg = ConfigLoader(Path(__file__).parent.parent / "config.yml")
    EntityExtractor(cfg).run()
