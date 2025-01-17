"""Закроем продукты (установим дату закрытия равную текущей) типа «КРЕДИТ»,
 у которых произошло полное погашение, но при этом не было повторной выдачи."""

from db_utils import connect_to_db, execute_sql
from psycopg2 import sql
import datetime

# Получаем текущую дату
current_date = datetime.date.today()

# Функция для закрытия продукта
def close_product(conn, product_id, close_date):
    update_query = sql.SQL("""
        UPDATE PRODUCTS
        SET CLOSE_DATE = %s
        WHERE ID = %s;
    """) # <-ЗДЕСЬ ИЗМЕНЕНИЯ: (добавил параметры)
    execute_sql(conn, update_query, params=(close_date, product_id)) # <-ЗДЕСЬ ИЗМЕНЕНИЯ: (добавил параметры в запрос)


# Создаем подключение к БД
conn = connect_to_db()

if conn:
    # SQL-запрос для выбора кредитных продуктов для закрытия
    select_query = sql.SQL("""
        SELECT
            p.ID AS product_id
        FROM PRODUCTS p
        JOIN PRODUCT_TYPE pt ON p.PRODUCT_TYPE_ID = pt.ID
        JOIN ACCOUNTS a ON p.ID = a.PRODUCT_REF
        WHERE pt.NAME = 'КРЕДИТ'
          AND NOT EXISTS (
              SELECT 1
              FROM RECORDS r
              WHERE r.ACC_REF = a.ID AND r.DT = 1
              GROUP BY r.ACC_REF
              HAVING COUNT(*) > 1
          )
        AND NOT EXISTS (
            SELECT 1
            FROM ACCOUNTS a2
            WHERE a2.PRODUCT_REF = p.ID AND a2.SALDO <> 0
        )
        AND p.CLOSE_DATE IS NULL
    """)

    # Выполняем SQL-запрос
    products_to_close = execute_sql(conn, select_query, fetch=True) # <-ЗДЕСЬ ИЗМЕНЕНИЯ: (добавил параметр fetch=True)

    if products_to_close:
        print("Продукты, которые будут закрыты:")
        for row in products_to_close:
            product_id = row[0]
            print(f"  ID продукта: {product_id}")

            close_product(conn, product_id, current_date)
            print("  Продукт закрыт!")
            print("-" * 30)
    else:
        print("Нет продуктов для закрытия.")
    
    conn.close()