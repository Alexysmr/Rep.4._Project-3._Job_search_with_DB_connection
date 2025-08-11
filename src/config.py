import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Пути к файлам
BASE_DIR = Path(__file__).resolve().parent.parent  # Корень проекта
DATA_DIR = BASE_DIR / "data"  # Директория с данными
LOGS_DIR = BASE_DIR / "logs"  # Директория с логами

# Настройка подключения к базе данных
load_dotenv(BASE_DIR / ".env")
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')

# Настройки API HH.ru
HH_API_URL = "https://api.hh.ru/vacancies"
HH_API_HEADERS = {"User-Agent": "MyVacancyParser/1.0 (alexxsmr@yandex.ru)"}
HH_API_AREA = 113  # Код региона по умолчанию 113 — Россия. Можно 1 - Москва, 2 - Санкт-Петербург, 78 - Самара и т.д.
PAGES = 1  # Количество запрашиваемых страниц по работодателю по умолчанию
PER_PAGE = 100  # Количество строк на странице
ID_COMPANY_ON_HHRU = {
    "Яндекс": 1740,
    "Сбер": 3529,
    "Тинькофф": 78638,
    "Ozon": 2180,
    "VK": 15478,
    "Kaspersky": 1057,
    "Авито": 84585,
    "Wildberries": 87021,
    "Газпром нефть": 39305,
    "Лукойл": 907345
}  # - коды компаний на hh.ru
ONLY_SALARY = 0  # Все варианты вакансий по зарплате, 1 - только с указанной зарплатой
DEFAULT_CURRENCY = "RUR"  # Валюта по умолчанию

# Настройки файлов
DEFAULT_JSON_FILE = "all_vacancies"  # Имя файла по умолчанию для сохранения полученных с hh.ru данных
CACHE_EXPIRE_HOURS = 1  # Время существования файла данных в часах

# Логирование
LOG_FORMAT = "%(asctime)s | %(levelname)s %(name)s, def: %(funcName)s, line:%(lineno)d, inf: %(message)s"


def setup_logging(logger_name: str) -> logging.Logger:
    """Централизованная конфигурация логирования для всего проекта"""
    log_filename = f"{logger_name.split('.')[0]}.log"
    log_filepath = LOGS_DIR / log_filename
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    file_handler = logging.FileHandler(log_filepath, "w", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)
    return logger
