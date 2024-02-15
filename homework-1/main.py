"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

# Параметры для подключения к базе данных
params_bd = {
    'host': 'localhost',
    'database': 'north',
    'user': 'evgeniagoncar',
    'password': '25171'
}


# Функция для чтения файлов .sql
def read_file_sql(file_sql):
    try:
        with open(file_sql) as f:
            reader = csv.reader(f)
            next(reader)
            return [line for line in reader]
    except Exception as er:
        print(f"Error: {er}")


# Функция для SQL запросов
def execute_query(param):
    # Подключение к базе данных
    conn = psycopg2.connect(**param)
    cur = conn.cursor()

    try:
        # Чтение файлов csv.
        employees_data = read_file_sql("north_data/employees_data.csv")
        customers_data = read_file_sql("north_data/customers_data.csv")
        orders_data = read_file_sql("north_data/orders_data.csv")

        # Передача файлов в таблицы
        for row in employees_data:
            cur.execute("INSERT INTO employees "
                        "(employee_id, first_name, last_name, title, birth_date, notes) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        row)
        for row in customers_data:
            cur.execute("INSERT INTO customers "
                        "(customer_id, company_name, contact_name) "
                        "VALUES (%s, %s, %s)",
                        row)
        for row in orders_data:
            cur.execute("INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city)"
                        " VALUES (%s, %s, %s, %s, %s)",
                        row)

        # Перадача в базу данных
        conn.commit()
        print("Запрос выполнен!")
    except Exception as er:
        print(er)
    finally:
        cur.close()
        conn.close()


execute_query(params_bd)
