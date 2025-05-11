# config.py

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv


class ConfigLoader:
    """Читает config.yml, подставляет ${ENV_VAR} и даёт доступ через [] и .paths."""

    # -------------------------- ИНИЦИАЛИЗАЦИЯ ------------------------------ #
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self._config = self._read_yaml()
        self._load_dotenv()

    # ----------------------------- ЗАГРУЗКА -------------------------------- #
    def _read_yaml(self) -> Dict[str, Any]:
        """Читает YAML и рекурсивно подставляет значения переменных окружения."""
        with open(self.config_path, encoding="utf-8") as fp:
            raw = yaml.safe_load(fp)
        return self._subst_env(raw)

    # ------------------ ПОДСТАНОВКА ${ENV_VAR} В YAML ---------------------- #
    def _subst_env(self, obj: Any) -> Any:
        """Обходит словари/списки и заменяет строки вида ${VAR} на os.environ."""
        if isinstance(obj, dict):
            return {k: self._subst_env(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._subst_env(item) for item in obj]
        if isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
            var = obj[2:-1]
            val = os.getenv(var)
            if val is None:
                raise ValueError(f"Переменная окружения {var} не установлена!")
            return val
        return obj

    # --------------------------- .env (секреты) ---------------------------- #
    def _load_dotenv(self) -> None:
        """Если в корне проекта лежит .env — загружаем его."""
        env_file = Path(__file__).parent.parent / ".env"
        if env_file.exists():
            load_dotenv(env_file)

    # ------------------------------ PATHS ---------------------------------- #
    @property
    def paths(self) -> Dict[str, Path]:
        """Возвращает словарь путей из config.yml, преобразованных в абсолютные."""
        root = Path(__file__).parent.parent
        return {k: (root / v).resolve() for k, v in self._config["paths"].items()}

    # ----------------------------- Доступ [] ------------------------------- #
    def __getitem__(self, key: str) -> Any:
        return self._config[key]
