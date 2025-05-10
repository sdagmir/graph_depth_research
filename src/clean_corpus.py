# src/clean_corpus.py

import re
from pathlib import Path
from tqdm import tqdm
from typing import List
from config import ConfigLoader


class CorpusCleaner:
    def __init__(self, config: ConfigLoader):
        self.config = config
        self._patterns = {
            'page_numbers': r'Page \d+ of \d+',
            'copyright': r'©.*?\d{4}',
            'latex_commands': r'\\\w+(\{.*?\})+',
            'block_formulas': r'\$\$.*?\$\$',
            'inline_formulas': r'\$.*?\$',
            'figures': r'Fig\.\s*\d+\.?\d*',
            'tables': r'Table\s*\d+\.?\d*',
            'hyphens': r'-\s+',
            'page_footers': r'^\d+$'
        }
        self._replacements = {
            'formulas': '[FORMULA]',
            'figures': '[FIGURE]',
            'tables': '[TABLE]'
        }

    def _clean_text(self, text: str) -> str:
        """Применяет все правила очистки из конфига"""
        text = re.sub(self._patterns['page_numbers'], '', text)
        text = re.sub(self._patterns['copyright'], '', text)
        text = re.sub(self._patterns['latex_commands'], '', text)

        text = re.sub(self._patterns['block_formulas'],
                      self._replacements['formulas'], text)
        text = re.sub(self._patterns['inline_formulas'],
                      self._replacements['formulas'], text)
        text = re.sub(self._patterns['figures'],
                      self._replacements['figures'], text)
        text = re.sub(self._patterns['tables'],
                      self._replacements['tables'], text)

        text = re.sub(self._patterns['hyphens'], '', text)
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(self._patterns['page_footers'],
                      '', text, flags=re.MULTILINE)

        return text.strip()

    def _process_file(self, input_path: Path) -> None:
        """Обрабатывает один файл"""
        try:
            content = input_path.read_text(encoding="utf-8")
            lines = content.split('\n')

            header = lines[0]
            cleaned = [self._clean_text(line) for line in lines[1:]]

            cleaned = [line for line in cleaned if line]

            output_path = self.config.paths["processed_corpus"] / \
                input_path.name
            output_path.write_text(
                '\n'.join([header] + cleaned), encoding="utf-8")

        except Exception as e:
            print(f"Error processing {input_path.name}: {str(e)}")

    def run_cleanup(self):
        """Основной пайплайн очистки"""
        input_dir = self.config.paths["interim_txt_dir"]
        output_dir = self.config.paths["processed_corpus"]

        output_dir.mkdir(parents=True, exist_ok=True)

        files = list(input_dir.glob("*.txt"))

        if not files:
            print(f"No TXT files found in {input_dir}")
            return

        for file_path in tqdm(files, desc="Cleaning corpus"):
            self._process_file(file_path)

        print(f"Cleaned {len(files)} files. Output in {output_dir}")


if __name__ == "__main__":
    config = ConfigLoader(Path(__file__).parent.parent / "config.yml")

    cleaner = CorpusCleaner(config)
    cleaner.run_cleanup()
