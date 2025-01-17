"""выборка, в которую попадут клиенты, у которых были операции по счетам за прошедший месяц от текущей даты.
 Выведите клиента и сумму операций за день в разрезе даты.
 (их нет, данные для базы устаревшие, на дворе стоял 2025 год)"""

from db_utils import connect_to_db, execute_sql
from psycopg2 import sql
import datetime

# Получаем текущую дату
current_date = datetime.date.today()

# Вычисляем дату месяц назад
one_month_ago = current_date - datetime.timedelta(days=30)

# Создаем подключение к БД
conn = connect_to_db()

if conn:
    # SQL-запрос для формирования отчета
    report_query = sql.SQL("""
        SELECT 
            c.NAME AS client_name,
            r.OPER_DATE AS operation_date,
            SUM(r.SUM) AS total_sum
        FROM CLIENTS c
        JOIN ACCOUNTS a ON c.ID = a.CLIENT_REF
        JOIN RECORDS r ON a.ID = r.ACC_REF
        WHERE r.OPER_DATE >= %s AND r.OPER_DATE <= %s
        GROUP BY c.NAME, r.OPER_DATE
        ORDER BY c.NAME, r.OPER_DATE
    """)

    # Выполняем SQL-запрос
    report_results = execute_sql(conn, report_query, fetch=True, params=(one_month_ago, current_date))

    if report_results:
        # Выводим результаты
        print("Отчет по операциям клиентов за последний месяц:")
        for row in report_results:
            print(f"  Имя клиента: {row[0]}")
            print(f"  Дата операции: {row[1]}")
            print(f"  Сумма операций: {row[2]}")
            print("-" * 30)
    else:
        print("Нет данных, удовлетворяющих условиям запроса.")
    
    conn.close()