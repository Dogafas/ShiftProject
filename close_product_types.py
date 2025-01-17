"""Закрытие возможности открытия (установите дату окончания действия) для типов продуктов,
 по счетам продуктов которых, не было движений более одного месяца."""
from db_utils import connect_to_db, execute_sql
from psycopg2 import sql
import datetime

# Получаем текущую дату
current_date = datetime.date.today()

# Вычисляем дату месяц назад
one_month_ago = current_date - datetime.timedelta(days=30)

# Функция для закрытия типа продукта
def close_product_type(conn, product_type_id, end_date):
    update_query = sql.SQL("""
        UPDATE PRODUCT_TYPE
        SET END_DATE = %s
        WHERE ID = %s;
    """) 
    execute_sql(conn, update_query, params=(end_date, product_type_id)) 


# Создаем подключение к БД
conn = connect_to_db()

if conn:
    # SQL-запрос для выбора типов продуктов для закрытия
    select_query = sql.SQL("""
        SELECT
            pt.ID AS product_type_id
        FROM PRODUCT_TYPE pt
        WHERE NOT EXISTS (
            SELECT 1
            FROM PRODUCTS p
            JOIN ACCOUNTS a ON p.ID = a.PRODUCT_REF
            JOIN RECORDS r ON a.ID = r.ACC_REF
            WHERE p.PRODUCT_TYPE_ID = pt.ID AND r.OPER_DATE >= %s
        )
        AND pt.END_DATE IS NULL
    """)

    # Выполняем SQL-запрос
    product_types_to_close = execute_sql(conn, select_query, fetch=True, params=(one_month_ago,)) 

    if product_types_to_close:
        print("Типы продуктов, для которых будет закрыта возможность открытия:")
        for row in product_types_to_close:
            product_type_id = row[0]
            print(f"  ID типа продукта: {product_type_id}")

            close_product_type(conn, product_type_id, current_date)
            print("  Тип продукта закрыт!")
            print("-" * 30)
    else:
        print("Нет типов продуктов для закрытия.")
    
    conn.close()