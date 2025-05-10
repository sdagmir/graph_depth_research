from pathlib import Path
import os
import yaml
from typing import Dict, Any
from dotenv import load_dotenv


class ConfigLoader:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self._config = self._load()
        self._load_secrets()  # Инициализация секретов

    def _load(self) -> Dict[str, Any]:
        """Загрузка YAML-конфига с подстановкой переменных окружения."""
        with open(self.config_path, 'r') as f:
            raw_config = yaml.safe_load(f)

        # Рекурсивная замена ${ENV_VAR} в значениях
        return self._replace_env_vars(raw_config)

    def _replace_env_vars(self, data: Any) -> Any:
        """Рекурсивно обрабатывает структуры данных, заменяя ${VAR} на значения из окружения."""
        if isinstance(data, dict):
            return {k: self._replace_env_vars(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._replace_env_vars(item) for item in data]
        elif isinstance(data, str) and data.startswith("${") and data.endswith("}"):
            env_var = data[2:-1]
            value = os.getenv(env_var)
            if value is None:
                raise ValueError(f"Environment variable {env_var} not set!")
            return value
        return data

    def _load_secrets(self):
        """Загружает .env файл из корня проекта (если существует)."""
        env_path = Path(__file__).parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)

    @property
    def paths(self) -> Dict[str, Path]:
        root = Path(__file__).parent.parent
        return {
            key: (root / path_str).resolve()
            for key, path_str in self._config['paths'].items()
        }

    def __getitem__(self, key: str) -> Any:
        return self._config[key]
