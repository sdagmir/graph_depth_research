# src/entity_extractor.py

import asyncio
import json
import re
from pathlib import Path
from typing import Dict, List, Optional

import textdistance
from dotenv import load_dotenv
from tqdm import tqdm

from config import ConfigLoader
from utils.llm import ask_openai

load_dotenv()


class EntityExtractor:
    """Извлечение специализированных терминов из текстовых лекций с помощью LLM."""

    # ------------------- КОНСТАНТЫ -----------------------------------------
    MAX_CHUNK_SIZE = 8_000
    DUP_THRESHOLD = 0.85
    STOPWORDS = {
        "App", "Application", "Data", "File", "System",
        "Приложение", "Данные", "Система", "Файл",
    }

    # ------------------- ИНИЦИАЛИЗАЦИЯ -------------------------------------
    def __init__(self, config: ConfigLoader):
        self.config = config
        self.corpus_dir = self.config.paths["processed_corpus"]
        project_root = Path(__file__).resolve().parent.parent
        self.output_path = (
            project_root / "data/processed/doc_entities.json").resolve()
        self.llm_params: Dict = self.config["llm"]

        self._validate_paths()

    def _validate_paths(self):
        if not self.corpus_dir.exists():
            raise FileNotFoundError(
                f"Каталог корпуса не найден: {self.corpus_dir}")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---------------------------
    @staticmethod
    def _to_camel_case(entity: str) -> str:
        entity = re.sub(r"[^\w\s]", "", entity)
        return "".join(w.capitalize() for w in entity.split())

    def _deduplicate(self, entities: List[str]) -> List[str]:
        uniq: List[str] = []
        for ent in entities:
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

    # ------------------- LLM ЗАПРОС (асинхронный) --------------------------
    async def _fetch_entities_async(self, text_chunk: str) -> List[str]:
        """Асинхронный запрос к LLM через utils.llm.ask_openai."""
        messages = [
            {
                "role": "system",
                "content": self.llm_params.get(
                    "system_prompt",
                    "Извлеки специализированные термины из текста. "
                    "Ответ верни СТРОГО JSON-массивом без комментариев.",
                ),
            },
            {"role": "user", "content": text_chunk},
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
            print(f"Ошибка LLM: {exc}")
            return []

    def _call_llm(self, text_chunk: str) -> List[str]:
        """Синхронная обёртка для использования в обычном коде."""
        return asyncio.run(self._fetch_entities_async(text_chunk))

    # ------------------- ОБРАБОТКА ОДНОГО ФАЙЛА ----------------------------
    def _process_file(self, file_path: Path) -> Optional[Dict[str, List[str]]]:
        try:
            content = file_path.read_text(encoding="utf-8")
            m = re.search(r"doc_id:\s*(\d+)", content)
            if not m:
                print(
                    f"В файле {file_path.name} отсутствует строка doc_id")
                return None

            doc_id = m.group(1)
            text_body = content.split("\n", 1)[1]

            chunks = [
                text_body[i: i + self.MAX_CHUNK_SIZE]
                for i in range(0, len(text_body), self.MAX_CHUNK_SIZE)
            ]

            raw_entities: List[str] = []
            for chunk in chunks:
                raw_entities.extend(self._call_llm(chunk))

            cleaned = self._deduplicate(
                [self._to_camel_case(e) for e in raw_entities])
            return {doc_id: cleaned}

        except Exception as exc:
            print(f"Ошибка при обработке {file_path.name}: {exc}")
            return None

    # ------------------- ОСНОВНОЙ ПАЙПЛАЙН ---------------------------------
    def run_pipeline(self):
        files = sorted(self.corpus_dir.glob("*.txt"))
        if not files:
            print("Документы в корпусе не найдены.")
            return

        result: Dict[str, List[str]] = {}
        for f in tqdm(files, desc="Извлечение сущностей"):
            doc_entities = self._process_file(f)
            if doc_entities:
                result.update(doc_entities)

        if not result:
            print("Не удалось извлечь ни одной сущности.")
            return

        tmp = self.output_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(result, ensure_ascii=False,
                       indent=2), encoding="utf-8")
        tmp.replace(self.output_path)
        print(f"Сущности сохранены в {self.output_path.resolve()}")


# ------------------- ЗАПУСК -------------------------------------------------
if __name__ == "__main__":
    cfg = ConfigLoader(Path(__file__).parent.parent / "config.yml")
    EntityExtractor(cfg).run_pipeline()
