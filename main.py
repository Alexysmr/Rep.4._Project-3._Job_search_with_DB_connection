import os

from src.api_client import HHAPIClient
from src.config import (
    CACHE_EXPIRE_HOURS,
    DATA_DIR,
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    DEFAULT_JSON_FILE,
    HH_API_AREA,
    ID_COMPANY_ON_HHRU,
    PAGES,
    setup_logging,
)
from src.database_processings import DBManager
from src.utils import check_exist_json_data, print_vacancies

modul_name = os.path.basename(__file__)
logger = setup_logging(modul_name)

if __name__ == "__main__":

    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST]):
        exit("Не заданы переменные окружения для подключения к БД. Проверьте .env файл и попробуйте снова.")

    print(
        "Здравствуйте!\nВас приветствует программа сбора данных о вакансиях с сайта hh.ru компаний:\nЯндекс, Сбер, "
        "Озон, Тинькофф, VK, Касперский, Авито, Wildberries, Газпром нефть, Лукойл."
    )
    search_word = input(
        "Для поиска в вакансиях введите ключевое слово (Enter без ввода слова выведет весь список "
        "полученных вакансий): "
    )
    print(" ")

    check_data = check_exist_json_data(
        file_path=DATA_DIR, file_name=DEFAULT_JSON_FILE, current_params=[ID_COMPANY_ON_HHRU, HH_API_AREA, 1]
    )
    choice_user = (
        input(
            f"Обнаружен файл актуальных данных с временем существования менее {CACHE_EXPIRE_HOURS}ч.\n"
            f"Enter - использовать его, любой символ + Enter - обновить файл данных: "
        )
        if check_data
        else False
    )

    if choice_user or not check_data:
        HHAPIClient(
            company_id_dict=ID_COMPANY_ON_HHRU,
            area=HH_API_AREA,
            pages=PAGES,
            salary=1,
            file_path=DATA_DIR,
            file_name=DEFAULT_JSON_FILE,
        )
        logger.info("Обновляем данные")

    db_manager = DBManager(file_path=DATA_DIR, file_name=DEFAULT_JSON_FILE)
    logger.info("Создаем экземпляр DBmanager")

    db_manager.create_database()
    logger.info("Создаем экземпляр БД")

    db_manager.create_tables()
    logger.info("Создаем экземпляр таблицы")

    db_manager.save_to_database()
    logger.info("Заполняем таблицы")

    print("\nКомпании и количество вакансий:")
    vacancies_count = db_manager.get_companies_and_vacancies_count()
    for item in vacancies_count:
        print(f"{item['name']}: {item['vacancies_count']} вакансий")

    print("\nСредняя зарплата:")
    print(db_manager.get_avg_salary())

    vacancies_higher = db_manager.get_vacancies_with_higher_salary()
    print(f"\nВакансий с зарплатой выше средней {len(vacancies_higher)}:\n")
    print_vacancies(vacancies_higher)

    vacancies_by_keyword = (
        db_manager.get_vacancies_with_keyword(search_word) if search_word else db_manager.get_all_vacancies()
    )
    if not vacancies_by_keyword:
        print("Соответствующие Вашему запросу вакансии не обнаружены.")
    else:
        print(f'\nВакансий по ключевому слову "{search_word.upper()}" {len(vacancies_by_keyword)}:\n')
        print_vacancies(vacancies_by_keyword)
