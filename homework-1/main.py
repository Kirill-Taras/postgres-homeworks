"""Скрипт для заполнения данными таблиц в БД Postgres."""

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
            return [line.strip() for line in f]
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
        cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", employees_data)
        cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", customers_data)
        cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders_data)

        # Перадача в базу данных
        conn.commit()
        print("Запрос выполнен!")
    except Exception as er:
        print(er)
    finally:
        cur.close()
        conn.close()


execute_query(params_bd)
