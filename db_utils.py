import os
from dotenv import load_dotenv
import psycopg2

# Загружаем переменные окружения из файла .env
load_dotenv()

# Получаем переменные окружения
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Функция для установки соединения с базой данных
def connect_to_db():
    try:
        conn = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB
        )
        return conn
    except psycopg2.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

# Функция для выполнения SQL-запроса
def execute_sql(conn, query, fetch=False, params=None):
    cur = conn.cursor()
    try:
        if params: # <-ЗДЕСЬ ИЗМЕНЕНИЯ: (если params есть передаем параметры в запрос)
            cur.execute(query, params)
        else:
             cur.execute(query)
        conn.commit()
        if fetch:
            results = cur.fetchall()
            return results
        else:
            return None
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Ошибка выполнения запроса: {e}")
        return None
    finally:
        cur.close()