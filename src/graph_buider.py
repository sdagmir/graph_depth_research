# src/graph_builder.py

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Set

import networkx as nx
from dotenv import load_dotenv
from tqdm import tqdm

from config import ConfigLoader

# --------------------------------------------------------------------------- #
#                              НАСТРОЙКИ ЛОГИРОВАНИЯ                          #
# --------------------------------------------------------------------------- #
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Допустимые типы отношений в графе
ALLOWED_RELS: Set[str] = {
    "defines",
    "uses",
    "depends_on",
    "implements",
    "part_of",
    "type_of",
    "runs_on",
    "opposite_of",
    "mentions",  # служебное ребро entity → DOC_x
}


class GraphBuilder:
    """Строит MultiDiGraph из корпуса и сохраняет его в нескольких форматах."""

    # --------------------------- ИНИЦИАЛИЗАЦИЯ ------------------------------ #
    def __init__(self, config: ConfigLoader):
        self.config = config
        self.graph = nx.MultiDiGraph()
        self._check_required_files()

    # ---------------------- ПРОВЕРКА НАЛИЧИЯ ФАЙЛОВ ------------------------- #
    def _check_required_files(self) -> None:
        """Убеждаемся, что все необходимые данные на месте."""
        corpus = self.config.paths["processed_corpus"]
        required = [
            corpus / "doc_entities.json",
            corpus / "triples.json",
        ]
        for p in required:
            if not p.exists():
                raise FileNotFoundError(f"Не найден файл: {p.resolve()}")

    # ----------------------------- ЗАГРУЗКА --------------------------------- #
    def _load_sources(self) -> Tuple[Dict[str, List[str]], List[List[str]]]:
        corpus = self.config.paths["processed_corpus"]
        with open(corpus / "doc_entities.json", encoding="utf-8") as fp:
            doc_entities = json.load(fp)
        with open(corpus / "triples.json", encoding="utf-8") as fp:
            triples = json.load(fp)
        return doc_entities, triples

    # ------------------------ ДОБАВЛЕНИЕ ТРИПЛЕТОВ -------------------------- #
    def _add_triples(self, triples: List[List[str]]) -> None:
        """Добавляет отношения entity → entity, удаляя дубликаты и невалидные."""
        seen: Set[Tuple[str, str, str]] = set()
        for head, rel, tail in tqdm(triples, desc="Добавляем триплеты"):
            if rel not in ALLOWED_RELS - {"mentions"}:
                logger.warning(
                    "Пропущено недопустимое отношение: %s %s %s", head, rel, tail)
                continue
            key = (head, rel, tail)
            if key in seen:
                continue
            seen.add(key)

            self.graph.add_node(head, type="entity")
            self.graph.add_node(tail, type="entity")
            self.graph.add_edge(head, tail, rel=rel)

    # ------------------------ СВЯЗЫВАНИЕ ДОКУМЕНТОВ ------------------------- #
    def _link_documents(self, doc_entities: Dict[str, List[str]]) -> None:
        """Связывает сущности с узлами документов ребром 'mentions'."""
        for doc_id, ents in tqdm(doc_entities.items(), desc="Связываем документы"):
            doc_node = f"DOC_{doc_id}"
            self.graph.add_node(doc_node, type="document", doc_id=int(doc_id))
            for ent in ents:
                self.graph.add_node(ent, type="entity")
                self.graph.add_edge(ent, doc_node, rel="mentions")

    # ----------------------------- СТАТИСТИКА ------------------------------- #
    def _print_stats(self) -> None:
        logger.info(
            "Граф построен: %d вершин, %d рёбер",
            self.graph.number_of_nodes(),
            self.graph.number_of_edges(),
        )
        sample = next(iter(self.graph.nodes))
        logger.info("Пример исходящих рёбер из «%s»:", sample)
        for _, tgt, data in list(self.graph.out_edges(sample, data=True))[:5]:
            logger.info("  → %s (%s)", tgt, data["rel"])

    # ----------------------------- СОХРАНЕНИЕ ------------------------------- #
    def _save(self) -> None:
        """Сохраняет граф в gpickle и gexf."""
        out_dir = self.config.paths["processed_corpus"] / "graph"
        out_dir.mkdir(exist_ok=True)
        nx.write_gpickle(self.graph, out_dir / "knowledge_graph.gpickle")
        nx.write_gexf(self.graph, out_dir / "knowledge_graph.gexf")
        logger.info("Граф сохранён в %s", out_dir.resolve())

    # ----------------------------- PIPELINE --------------------------------- #
    def run(self) -> None:
        doc_entities, triples = self._load_sources()
        self._add_triples(triples)
        self._link_documents(doc_entities)
        self._print_stats()
        self._save()
        logger.info("Построение графа знаний завершено.")


# ------------------------------ ЗАПУСК ------------------------------------- #
if __name__ == "__main__":
    cfg = ConfigLoader(Path(__file__).parent.parent / "config.yml")
    GraphBuilder(cfg).run()
