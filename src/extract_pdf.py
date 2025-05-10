# src/extract_pdf.py

from pathlib import Path
import re
from tqdm import tqdm
import pdfplumber
from config import ConfigLoader


class PDFExtractor:
    def __init__(self, config: ConfigLoader):
        self.config = config
        self._validate_paths()

    def _validate_paths(self):
        """Проверяем существование необходимых директорий"""
        if not self.config.paths["raw_pdf_dir"].exists():
            raise FileNotFoundError(
                f"PDF directory not found: {self.config.paths['raw_pdf_dir']}")

        self.config.paths["interim_txt_dir"].mkdir(parents=True, exist_ok=True)

    def _clean_text(self, text: str) -> str:
        """Применяет все правила очистки из конфига"""
        patterns = self.config["processing"]["regex_patterns"]
        replacements = self.config["processing"]["replacements"]

        text = re.sub(patterns["whitespace"], ' ', text)
        text = re.sub(patterns["formula"], replacements["formula"], text)
        text = re.sub(patterns["latex_commands"], '', text)
        return text.strip()

    def process_all(self):
        """Основной пайплайн обработки"""
        pdf_files = list(self.config.paths["raw_pdf_dir"].glob("*.pdf"))

        if not pdf_files:
            print(f"No PDF files found in {self.config.paths['raw_pdf_dir']}")
            return

        for idx, pdf_path in enumerate(tqdm(pdf_files, desc="Processing PDFs")):
            doc_id = idx + 1
            output_path = self.config.paths["interim_txt_dir"] / \
                f"{doc_id}.txt"
            self._process_single(pdf_path, output_path, doc_id)

    def _process_single(self, pdf_path: Path, output_path: Path, doc_id: int):
        """Обрабатывает один PDF-файл"""
        text_content = [f"doc_id: {doc_id}"]

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    text = self._clean_text(text)
                    if text:
                        text_content.append(text)

            output_path.write_text("\n".join(text_content), encoding="utf-8")
            print(f"Success: {pdf_path.name} → {output_path}")

        except Exception as e:
            print(f"Error processing {pdf_path.name}: {str(e)}")


if __name__ == "__main__":

    config_path = Path(__file__).parent.parent / "config.yml"
    config = ConfigLoader(config_path)

    extractor = PDFExtractor(config)
    extractor.process_all()
