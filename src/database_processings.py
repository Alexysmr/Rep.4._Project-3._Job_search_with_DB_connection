import os
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import psycopg2
from psycopg2 import sql

from src.config import DATA_DIR, DB_HOST, DB_NAME, DB_PASSWORD, DB_USER, DEFAULT_JSON_FILE, setup_logging
from src.utils import read_json_data

modul_name = os.path.basename(__file__)
logger = setup_logging(modul_name)


class DBManager:
    """Получение данных с hh.ru и распределение данных по таблицам в соответствии с методами"""

    def __init__(self, file_path: Path = DATA_DIR, file_name: str = DEFAULT_JSON_FILE):
        """Инициализация подключения к БД и получения данных"""
        self.conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
        self.conn.autocommit = True
        self.file_path = file_path
        self.file_name = file_name
        logger.info("Инициализатор")
        self.all_data = read_json_data(self.file_path, self.file_name)
        if not isinstance(self.all_data, list) or len(self.all_data) < 2:
            logger.error("Некорректный формат данных")
            self.all_data = [{}, {"_metadata": {}}]
        self.all_vacancies = self.all_data[0].get("data", [])
        self.company = self.all_data[1].get("_metadata", {}).get("company_id_dict", {})
        logger.info("Все вакансии и список компаний из _metadata получены")

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Итерируем список вакансий"""
        return iter(self.all_vacancies)

    def __del__(self) -> None:
        """Закрытие подключения при удалении объекта"""
        if hasattr(self, "conn"):
            self.conn.close()
            logger.info("Подключение к БД закрыто")

    def _execute_query(self, query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Общий метод выполнения SQL-запросов."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params or [])
                if cur.description is None:
                    return []
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
        except psycopg2.Error as er:
            self.conn.rollback()
            logger.error(f"Ошибка: {er}")
            return []

    def create_database(self, database_name: str = "hh_vacancies") -> None:
        """Проверяет существование БД с заданным именем, создает при ее отсутствии и совершает переподключение
        на заданную БД"""
        logger.info(f"Старт. Создание БД {database_name} если не существует")
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (database_name,))
            db_exists = cur.fetchone()
            if not db_exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
                logger.info(f"БД {database_name} создана")
        self.conn.close()
        self.conn = psycopg2.connect(dbname=database_name, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
        logger.info(f"Переключение на БД {database_name}")
        self.conn.autocommit = True

    def create_tables(self) -> None:
        """Пересоздание или создание таблиц в БД"""
        with self.conn.cursor() as cur:
            try:
                # Удаляем таблицу employers если существует
                cur.execute("""DROP TABLE IF EXISTS employers CASCADE;""")
                # Создание таблицы employers
                cur.execute(
                    """
                    CREATE TABLE employers (
                        employer_id SERIAL PRIMARY KEY,
                        hh_id INTEGER UNIQUE NOT NULL,
                        name VARCHAR(255) NOT NULL);
                """
                )
                # Удаляем таблицу vacancies если существует
                cur.execute("""DROP TABLE IF EXISTS vacancies;""")
                # Создание таблицы vacancies
                cur.execute(
                    """
                    CREATE TABLE vacancies (
                        vacancy_id SERIAL PRIMARY KEY,
                        employer_id INTEGER REFERENCES employers(employer_id),
                        title VARCHAR(255) NOT NULL,
                        salary_from INTEGER,
                        salary_to INTEGER,
                        currency VARCHAR(10),
                        url VARCHAR(512) UNIQUE,
                        CONSTRAINT fk_employer FOREIGN KEY(employer_id) REFERENCES employers(employer_id));
                """
                )
                logger.info("Таблицы employers и vacancies созданы")
            except psycopg2.Error as er:
                logger.error(f"Ошибка создания таблиц: {er}")
                raise

    def save_to_database(self) -> None:
        """Сохранение данных полученных с hh.ru API в БД в требуемой архитектуре"""
        with self.conn.cursor() as cur:
            try:
                # Вставка работодателей
                for company_name, hh_id in self.company.items():
                    cur.execute(
                        "INSERT INTO employers (hh_id, name) VALUES (%s, %s) ON CONFLICT (hh_id) DO NOTHING",
                        (hh_id, company_name),
                    )
                # Вставка вакансий
                for vacancy in self.all_vacancies:
                    salary = vacancy.get("salary", {})
                    if not salary or vacancy.get("salary", {}).get("currency") != "RUR":
                        continue
                    cur.execute(
                        """
                        INSERT INTO vacancies (employer_id, title, salary_from, salary_to, currency, url)
                        VALUES (
                            (SELECT employer_id FROM employers WHERE hh_id = %s),
                            %s, %s, %s, %s, %s
                        ) ON CONFLICT (url) DO NOTHING
                        """,
                        (
                            vacancy["employer"]["id"],
                            vacancy["name"],
                            salary.get("from"),
                            salary.get("to"),
                            salary.get("currency"),
                            vacancy["alternate_url"],
                        ),
                    )
                logger.info(f"Данные сохранены в БД, вакансий: {len(list(self.all_vacancies))}")
            except psycopg2.Error as er:
                logger.error(f"Ошибка сохранения данных: {er}")
                raise

    def get_companies_and_vacancies_count(self) -> List[Dict[str, Any]]:
        """Получает список всех компаний и количество вакансий у каждой компании"""
        query = """
            SELECT e.name, COUNT(v.vacancy_id) as vacancies_count
            FROM employers e
            LEFT JOIN vacancies v ON e.employer_id = v.employer_id
            GROUP BY e.name
            ORDER BY vacancies_count DESC
        """
        return self._execute_query(query)

    def get_all_vacancies(self) -> List[Dict[str, Any]]:
        """Получает список всех вакансий с указанием названия компании, названия вакансии, зарплаты(от - до)
        и ссылки на вакансию"""
        query = """
            SELECT e.name as company, v.title, 
                    v.salary_from, v.salary_to, v.currency, v.url
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.employer_id
            ORDER BY e.name, (v.salary_from + v.salary_to)/2 DESC
        """
        return self._execute_query(query)

    def get_avg_salary(self) -> float:
        """Получает среднюю зарплату по вакансиям имеющим значения зарплаты 'от' и 'до', остальные игнорируются"""
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT AVG((salary_from + salary_to)/2) as avg_salary
                    FROM vacancies
                    WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL
                """
                )
                result = cur.fetchone()
                return round(result[0] if result else 0, 2)
            except psycopg2.Error as er:
                logger.error(f"Ошибка расчета средней зарплаты: {er}")
                return 0.0

    def get_vacancies_with_higher_salary(self) -> List[Dict[str, Any]]:
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        query = """
            SELECT e.name as company, v.title, 
                    v.salary_from, v.salary_to, v.currency, v.url
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.employer_id
            WHERE (v.salary_from + v.salary_to)/2 > (
                SELECT AVG((salary_from + salary_to)/2) 
                FROM vacancies
                WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL
            )
            ORDER BY (v.salary_from + v.salary_to)/2 DESC
        """
        return self._execute_query(query)

    def get_vacancies_with_keyword(self, keyword: str) -> List[Dict[str, Any]]:
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        query = """
            SELECT e.name as company, v.title, 
                    v.salary_from, v.salary_to, v.currency, v.url
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.employer_id
            WHERE v.title ILIKE %s
            ORDER BY (v.salary_from + v.salary_to)/2 DESC
        """
        return self._execute_query(query, [f"%{keyword}%"])
