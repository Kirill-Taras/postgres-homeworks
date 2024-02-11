"""Скрипт для заполнения данными таблиц в БД Postgres."""

import pandas as pd
import psycopg2

# Параметры для подключения к базе данных
params_bd = {
    'host': 'localhost',
    'database': 'north',
    'user': 'evgeniagoncar',
    'password': '25171'
}


# Функция для SQL запросов
def execute_query(param):
    # Подключение к базе данных
    conn = psycopg2.connect(**param)
    cur = conn.cursor()

    try:
        # Чтение файлов csv.
        employees_data = pd.read_csv("north_data/employees_data.csv")
        customers_data = pd.read_csv("north_data/customers_data.csv")
        orders_data = pd.read_csv("north_data/orders_data.csv")

        # Передача файлов в таблицы
        cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", employees_data.values)
        cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", customers_data.values)
        cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_data.values)

        # Перадача в базу данных
        conn.commit()
        print("Запрос выполнен!")
    except Exception as er:
        print(er)
    finally:
        cur.close()
        conn.close()


execute_query(params_bd)
