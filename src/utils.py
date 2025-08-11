import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import CACHE_EXPIRE_HOURS, DATA_DIR, DEFAULT_JSON_FILE, setup_logging

modul_name = os.path.basename(__file__)
logger = setup_logging(modul_name)


def check_exist_json_data(file_path: Path | None = None, file_name: str | None = None,
                          current_params: Optional[List[Dict] | Any] = None) -> bool:
    """Проверяет существование файла с данными соответствующих запросу и, если он существует менее часа,
     возвращает True, в противном случае False"""
    data_file = file_path / f"{file_name}.json"
    logger.info(f"Старт проверки существования {data_file}")

    if not data_file.exists() or (
            datetime.now() - datetime.fromtimestamp(data_file.stat().st_mtime)).seconds / 3600 >= CACHE_EXPIRE_HOURS:
        logger.info(f"Файл {data_file} отсутствует, либо устарел")
        return False
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            content = json.load(f)
        if not content[0].get("data") or not content[1].get("_metadata"):
            logger.info("Отсутствуют вакансии или метаданные")
            return False
        if current_params:
            logger.info("Проверка данных на соответствие параметрам запроса")
            if [content[1].get("_metadata").get("company_id_dict"), content[1].get("_metadata").get("area"),
                content[1].get("_metadata").get("salary")] != current_params:
                return False
    except (json.JSONDecodeError, ValueError):
        return False
    logger.info(f"Файл {data_file} - JSON-файл с актуальными данными")
    return True


def read_json_data(file_path: Path, file_name: str) -> List[Dict[str, Any]]:
    """Функция чтения json-файла"""
    data_file = file_path / f"{file_name}.json"
    try:
        with open(data_file, "r", encoding="utf-8") as f:
            logger.info(f"Данные из {data_file} успешно загружены")
            return json.load(f)
    except json.JSONDecodeError:
        logger.error("Ошибка чтения файла")
        exit("Ошибка чтения файла данных. Попробуйте запустить программу снова.")


def overwriting_json_data(data: Dict[str, Any] | None = None, file_path: Path = DATA_DIR,
                          file_name: str = DEFAULT_JSON_FILE, metadata: Optional[Dict[str, Any]] = None):
    """Функция записи/перезаписи json данных в файл"""
    data_file = file_path / f"{file_name}.json"
    data_to_save = [data, metadata]
    with open(data_file, "w", encoding="utf-8") as file:
        json.dump(data_to_save, file, indent=4, ensure_ascii=False)
        logger.info(f"Данные сохранены в {data_file}")
