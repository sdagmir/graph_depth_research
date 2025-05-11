# src/clean_corpus.py

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List

from tqdm import tqdm

from config import ConfigLoader

# ---------------------------- ЛОГИРОВАНИЕ --------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class CorpusCleaner:
    """Читает файлы из interim_txt_dir, чистит по правилам и кладёт в processed_corpus."""

    # ------------------------- ИНИЦИАЛИЗАЦИЯ ------------------------------- #
    def __init__(self, config: ConfigLoader) -> None:
        self.config = config
        self._patterns = {
            "page_numbers": r"Page \d+ of \d+",
            "copyright": r"©.*?\d{4}",
            "latex_commands": r"\\\w+(\{.*?\})+",
            "block_formulas": r"\$\$.*?\$\$",
            "inline_formulas": r"\$.*?\$",
            "figures": r"Fig\.\s*\d+\.?\d*",
            "tables": r"Table\s*\d+\.?\d*",
            "hyphens": r"-\s+",
            "page_footers": r"^\d+$",
        }
        self._replacements = {
            "formulas": "[FORMULA]",
            "figures": "[FIGURE]",
            "tables": "[TABLE]",
        }

    # -------------------------- ОЧИСТКА ТЕКСТА ----------------------------- #
    def _clean_line(self, text: str) -> str:
        t = text
        t = re.sub(self._patterns["page_numbers"], "", t)
        t = re.sub(self._patterns["copyright"], "", t)
        t = re.sub(self._patterns["latex_commands"], "", t)

        t = re.sub(self._patterns["block_formulas"],
                   self._replacements["formulas"], t)
        t = re.sub(self._patterns["inline_formulas"],
                   self._replacements["formulas"], t)
        t = re.sub(self._patterns["figures"], self._replacements["figures"], t)
        t = re.sub(self._patterns["tables"], self._replacements["tables"], t)

        t = re.sub(self._patterns["hyphens"], "", t)
        t = re.sub(r"\s+", " ", t)
        t = re.sub(self._patterns["page_footers"], "", t, flags=re.MULTILINE)
        return t.strip()

    # ----------------------- ОБРАБОТКА ОДНОГО ФАЙЛА ------------------------ #
    def _process_file(self, src: Path, dst: Path) -> None:
        try:
            raw = src.read_text(encoding="utf-8")
            header, *body = raw.split("\n")
            cleaned_body: List[str] = [
                self._clean_line(line) for line in body if line]
            dst.write_text(
                "\n".join([header] + cleaned_body), encoding="utf-8")
        except Exception as exc:
            logger.error("Ошибка при обработке %s: %s", src.name, exc)

    # -------------------------- ОСНОВНОЙ PIPELINE -------------------------- #
    def run(self) -> None:
        src_dir = self.config.paths["interim_txt_dir"]
        dst_dir = self.config.paths["processed_corpus"]

        dst_dir.mkdir(parents=True, exist_ok=True)
        files = sorted(src_dir.glob("*.txt"))

        if not files:
            logger.warning("В каталоге %s нет *.txt файлов.",
                           src_dir.resolve())
            return

        for fp in tqdm(files, desc="Очистка корпуса"):
            self._process_file(fp, dst_dir / fp.name)

        logger.info("Очистка завершена. Файлов обработано: %d. Результат: %s", len(
            files), dst_dir.resolve())


# ------------------------------ ЗАПУСК ------------------------------------- #
if __name__ == "__main__":
    cfg = ConfigLoader(Path(__file__).parent.parent / "config.yml")
    CorpusCleaner(cfg).run()
