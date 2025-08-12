import os
import time
import requests
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Any, List, Dict

from src.config import DATA_DIR, HH_API_AREA, HH_API_HEADERS, HH_API_URL, PAGES, PER_PAGE, ID_COMPANY_ON_HHRU, \
    ONLY_SALARY, DEFAULT_CURRENCY, DEFAULT_JSON_FILE, setup_logging
from src.utils import overwriting_json_data

modul_name = os.path.basename(__file__)
logger = setup_logging(modul_name)


class AbstractAPIClient(ABC):
    @abstractmethod
    def get_vacancies(self, company_id_dict: Dict, area: int, pages: int, salary: Any) -> List[Dict]:
        pass


class HHAPIClient(AbstractAPIClient):
    """Класс создаёт файл с полученными по запросу данными о вакансиях. В сценарий по умолчанию заложено:
    запрос первой страницы с 100 вакансиями на страницу. Все полученные 'сырые' данные
    записываются в файл. В случае возникновения ошибок в получении данных с сайта выбрасывается исключение"""

    logger.info("Старт api-клиента")
    BASE_URL = HH_API_URL

    def __init__(
            self,
            company_id_dict: Dict = ID_COMPANY_ON_HHRU,
            area: int = HH_API_AREA,
            pages: int = PAGES,
            salary: Any = ONLY_SALARY,
            file_path: Path = DATA_DIR,
            file_name: str = DEFAULT_JSON_FILE
    ):
        self.company = company_id_dict
        self.area = area
        self.pages = pages
        self.per_page = PER_PAGE
        self.headers = HH_API_HEADERS
        self.salary = salary if salary else ONLY_SALARY
        self.file_path = Path(file_path)
        self.file_name = file_name
        logger.info(f"Инициализатор. Зона охвата вакансий - {self.area}, статус 'Только с зарплатой' - {self.salary}")
        self.all_info = self.get_vacancies(self.company, self.area, self.pages, self.salary)

    def __repr__(self):
        return (f"company_id_dict: {self.company},\narea: {self.area},\npages: {self.pages},"
                f"\nper pages: {self.per_page},\nheaders: {self.headers},\nonly salary: {self.salary}."
                f"\nfile name: {self.file_name}.json")

    def __iter__(self):
        return iter(self.all_info)

    def get_vacancies(self, company_id_dict: Dict, area: int, pages: int, salary: Any) -> list | None:
        """Получение списка вакансий по списку компаний-работодателей
        company_id_dict - словарь "Название_работодателя": "код_работодателя_на_hh.ru"(см. config.py);
        area - город расположения вакансий, по умолчанию 113 - Россия, можно задать города;
        pages - количество страниц вакансий по каждому работодателю, по умолчанию 1 страница;
        param salary: любой символ — только с зарплатой, ""(None) — все вакансии.
        """
        with_salary = 1 if salary is not None else 0
        if not company_id_dict:
            logger.warning("Пустой словарь компаний!. Exit")
            exit("Отсутствуют данные для запроса по компаниям. Работа программы завершена.\n"
                 "Проверьте ID_COMPANY_ON_HHRU в config.py или укажите другой и попробуйте снова.")
        remaining_requests = len(company_id_dict) * pages  # Общее число запросов
        delay = 0.5 if remaining_requests >= 20 else 0.1  # уважаем чужой API
        logger.info(f"Старт. Общее количество запросов = {remaining_requests}, delay = {delay}")
        all_vacancies = []
        try:
            for key, values in company_id_dict.items():
                company_vacancies = []
                for p in range(pages):
                    remaining_requests -= 1
                    current_params = {
                        "employer_id": values,
                        "per_page": self.per_page,
                        "page": p,
                        "currency": DEFAULT_CURRENCY,
                        "only_with_salary": with_salary
                    }
                    logger.info(f"Запрос {key} стр.{p}")
                    response = requests.get(
                        self.BASE_URL, params=current_params, headers=self.headers
                    )  # Передаём заголовки
                    response.raise_for_status()
                    query_data = response.json().get("items", [])
                    if remaining_requests > 0:
                        time.sleep(delay)
                    if query_data:
                        company_vacancies.extend(query_data)
                        logger.info(f"Страница {p} {key} получена")
                    if p + 1 == pages and company_vacancies:
                        all_vacancies.extend(company_vacancies)
                        logger.info(f"Сбор данных для {key} завершён")
            logger.info(f"Общий сбор данных завершён вакансий собрано {len(all_vacancies)}")
            data = {"data": all_vacancies}
            metadata = {
                "_metadata": {
                    "company_id_dict": company_id_dict,
                    "area": area,
                    "salary": salary
                }
            }
            overwriting_json_data(data, self.file_path, self.file_name, metadata)
        except requests.exceptions.RequestException as err:
            logger.warning(f"Ошибка запроса: {err}. Exit.")
            exit(f"Ошибка запроса: {err}. Работа программы прекращена:\nНе удалось получить данные с сайта hh.ru, "
                 f"проверьте соединение с интернетом и попробуйте снова.")
