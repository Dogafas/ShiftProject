"""отчет, который содержит все счета, относящиеся к продуктам типа ДЕПОЗИТ,
 принадлежащих клиентам, у которых нет открытых продуктов типа КРЕДИТ
"""
from db_utils import connect_to_db, execute_sql
from psycopg2 import sql

# Создаем подключение к БД
conn = connect_to_db()

if conn:
    # SQL-запрос для формирования отчета
    report_query = sql.SQL("""
        SELECT 
            a.NAME AS account_name,
            a.ACC_NUM AS account_number,
            a.SALDO AS account_saldo,
            c.NAME AS client_name
        FROM ACCOUNTS a
        JOIN PRODUCTS p ON a.PRODUCT_REF = p.ID
        JOIN PRODUCT_TYPE pt ON p.PRODUCT_TYPE_ID = pt.ID
        JOIN CLIENTS c ON a.CLIENT_REF = c.ID
        WHERE pt.NAME = 'ДЕПОЗИТ'
        AND c.ID NOT IN (
            SELECT p2.CLIENT_REF
            FROM PRODUCTS p2
            JOIN PRODUCT_TYPE pt2 ON p2.PRODUCT_TYPE_ID = pt2.ID
            WHERE pt2.NAME = 'КРЕДИТ'
        );
    """)

    # Выполняем SQL-запрос
    report_results = execute_sql(conn, report_query, fetch=True) 
    
    if report_results:
        # Выводим результаты
        print("Отчет по счетам депозитных продуктов для клиентов без кредитов:")
        for row in report_results:
            print(f"  Имя счета: {row[0]}")
            print(f"  Номер счета: {row[1]}")
            print(f"  Остаток: {row[2]}")
            print(f"  Имя клиента: {row[3]}")
            print("-" * 30)
    else:
        print("Нет данных, удовлетворяющих условиям запроса.")
    
    conn.close()