"""выборка, которая возвращает информацию о клиентах,
которые полностью погасили кредит, но при этом не закрыли продукт"""
from db_utils import connect_to_db, execute_sql
from psycopg2 import sql

# Создаем подключение к БД
conn = connect_to_db()

if conn:
    # SQL-запрос для формирования отчета
    report_query = sql.SQL("""
        SELECT
            c.NAME AS client_name,
            p.NAME AS product_name,
            a.NAME AS account_name,
             a.SALDO AS account_saldo,
            p.CLOSE_DATE AS product_close_date
        FROM CLIENTS c
        JOIN PRODUCTS p ON c.ID = p.CLIENT_REF
        JOIN PRODUCT_TYPE pt ON p.PRODUCT_TYPE_ID = pt.ID
        JOIN ACCOUNTS a ON p.ID = a.PRODUCT_REF
        WHERE pt.NAME = 'КРЕДИТ'
          AND a.SALDO = 0
          AND p.CLOSE_DATE IS NULL;
    """)

    # Выполняем SQL-запрос
    report_results = execute_sql(conn, report_query, fetch=True) 

    if report_results:
        # Выводим результаты
        print("Отчет по клиентам, которые погасили кредит, но не закрыли продукт:")
        for row in report_results:
            print(f"  Имя клиента: {row[0]}")
            print(f"  Наименование продукта: {row[1]}")
            print(f"  Наименование счета: {row[2]}")
            print(f"  Остаток по счету: {row[3]}")
            print(f"  Дата закрытия продукта: {row[4]}")
            print("-" * 30)
    else:
        print("Нет данных, удовлетворяющих условиям запроса.")
    
    conn.close()