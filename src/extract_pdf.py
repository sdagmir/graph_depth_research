# src/extract_pdf.py

from __future__ import annotations

import logging
import re
from pathlib import Path

import pdfplumber
from dotenv import load_dotenv
from tqdm import tqdm

from config import ConfigLoader

# ----------------------------- ЛОГИРОВАНИЕ -------------------------------- #
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class PDFExtractor:
    """Читает PDF-файлы и сохраняет очищенный текст в interim_txt_dir."""

    # --------------------------- ИНИЦИАЛИЗАЦИЯ ----------------------------- #
    def __init__(self, config: ConfigLoader):
        self.config = config
        self._check_dirs()

    # ---------------------- ПРОВЕРКА/СОЗДАНИЕ КАТАЛОГОВ -------------------- #
    def _check_dirs(self) -> None:
        """Убеждаемся, что исходная и целевая директории существуют."""
        pdf_dir = self.config.paths["raw_pdf_dir"]
        txt_dir = self.config.paths["interim_txt_dir"]

        if not pdf_dir.exists():
            raise FileNotFoundError(
                f"Каталог PDF не найден: {pdf_dir.resolve()}")

        txt_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------- ОЧИСТКА ТЕКСТА ---------------------------- #
    def _clean(self, text: str) -> str:
        """Применяет регэкспы из config.yml к извлечённому тексту."""
        rx = self.config["processing"]["regex_patterns"]
        rep = self.config["processing"]["replacements"]

        text = re.sub(rx["whitespace"], " ", text)
        text = re.sub(rx["formula"], rep["formula"], text)
        text = re.sub(rx["latex_commands"], "", text)
        return text.strip()

    # ----------------------------- ОБРАБОТКА ------------------------------- #
    def _process_pdf(self, pdf_path: Path, out_path: Path, doc_id: int) -> None:
        """Читает один PDF и пишет очищенный текст в файл."""
        lines = [f"doc_id: {doc_id}"]

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    raw = page.extract_text() or ""
                    cleaned = self._clean(raw)
                    if cleaned:
                        lines.append(cleaned)

            out_path.write_text("\n".join(lines), encoding="utf-8")
            logger.info("Успешно: %s → %s", pdf_path.name, out_path.name)
        except Exception as exc:
            logger.error("Ошибка при обработке %s: %s", pdf_path.name, exc)

    def run(self) -> None:
        """Основной пайплайн: проходим по всем PDF в raw_pdf_dir."""
        pdf_dir = self.config.paths["raw_pdf_dir"]
        pdf_files = sorted(pdf_dir.glob("*.pdf"))

        if not pdf_files:
            logger.warning(
                "В каталоге %s не найдено PDF-файлов.", pdf_dir.resolve())
            return

        for idx, pdf in enumerate(tqdm(pdf_files, desc="Обработка PDF")):
            doc_id = idx + 1
            out_path = self.config.paths["interim_txt_dir"] / f"{doc_id}.txt"
            self._process_pdf(pdf, out_path, doc_id)

        logger.info("Извлечение текста завершено.")


# ----------------------------- ЗАПУСК -------------------------------------- #
if __name__ == "__main__":
    cfg = ConfigLoader(Path(__file__).parent.parent / "config.yml")
    PDFExtractor(cfg).run()
