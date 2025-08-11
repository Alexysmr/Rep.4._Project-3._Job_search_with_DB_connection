import os

from src.config import (DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DATA_DIR, HH_API_AREA, PAGES, ID_COMPANY_ON_HHRU,
                        DEFAULT_JSON_FILE, CACHE_EXPIRE_HOURS, setup_logging)
from src.database_processings import DBManager
from src.utils import check_exist_json_data
from src.api_client import HHAPIClient

modul_name = os.path.basename(__file__)
logger = setup_logging(modul_name)


def print_vacancies(vacancies):
    """Вывод результатов в консоль"""
    for v in vacancies:
        print(f"Компания: {v['company']}\nДолжность: {v['title']}\nЗарплата: {v['salary_from']}-{v['salary_to']} "
              f"{v['currency']}\nСсылка: {v['url']}\n")


if __name__ == "__main__":

    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST]):
        exit("Не заданы переменные окружения для подключения к БД. Проверьте .env файл и попробуйте снова.")

    print("Здравствуйте!\nВас приветствует программа сбора данных о вакансиях с сайта hh.ru компаний:\nЯндекс, Сбер, "
          "Озон, Тинькофф, VK, Касперский, Авито, Wildberries, Газпром нефть, Лукойл.")
    search_word = input(
        "Для поиска в вакансиях введите ключевое слово (Enter без ввода слова выведет весь список "
        "полученных вакансий): ")
    print(" ")

    query_params = [
        ID_COMPANY_ON_HHRU,
        HH_API_AREA,
        PAGES,
        1,  # статус ONLY_SALARY
        DATA_DIR,
        DEFAULT_JSON_FILE
    ]
    check_data = check_exist_json_data(
        file_path=query_params[4],
        file_name=query_params[5],
        current_params=[query_params[0], query_params[1], query_params[3]]
    )
    choice_user = input(f"Обнаружен файл актуальных данных с временем существования менее {CACHE_EXPIRE_HOURS}ч.\n"
                        f"Enter - использовать его, любой символ + Enter - обновить файл данных: ") if check_data \
        else False

    if choice_user or not check_data:
        HHAPIClient(company_id_dict=query_params[0],
                    area=query_params[1],
                    pages=query_params[2],
                    salary=query_params[3],
                    file_path=query_params[4],
                    file_name=query_params[5])
        logger.info("Обновляем данные")

    db_manager = DBManager(file_path=DATA_DIR, file_name=DEFAULT_JSON_FILE)

    db_manager.create_database()

    db_manager.create_tables()

    db_manager.save_to_database()

    print("\nКомпании и количество вакансий:")
    vacancies_count = db_manager.get_companies_and_vacancies_count()
    for item in vacancies_count:
        print(f"{item['name']}: {item['vacancies_count']} вакансий")

    print("\nСредняя зарплата:")
    print(db_manager.get_avg_salary())

    vacancies_higher = db_manager.get_vacancies_with_higher_salary()
    print(f"\nВакансий с зарплатой выше средней {len(vacancies_higher)}:\n")
    print_vacancies(vacancies_higher)

    vacancies_by_keyword = db_manager.get_vacancies_with_keyword(
        search_word) if search_word else db_manager.get_all_vacancies()
    if not vacancies_by_keyword:
        print("Соответствующие Вашему запросу вакансии не обнаружены.")
    else:
        print(f"\nВакансий по ключевому слову \"{search_word.upper()}\" {len(vacancies_by_keyword)}:\n")
        print_vacancies(vacancies_by_keyword)
