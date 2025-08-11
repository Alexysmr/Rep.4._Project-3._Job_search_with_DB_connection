**Описание**
Курсовая работа Проект 3. Поиск вакансий с подключением БД по специальности "Разработчик Python" университета Skypro.

Задачи выполняемые программой: получение данных о вакансиях с сайта hh.ru, создание базы данных, таблиц и заполнение таблиц 
в соответствии с заданием по курсовой работе.

Проект содержит 5 модулей

    CASE
        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to)/2
        WHEN salary_from IS NOT NULL THEN salary_from  -- Берём только нижнюю границу
        WHEN salary_to IS NOT NULL THEN salary_to     -- Берём только верхнюю границу
    END