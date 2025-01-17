"""(Нормализация данных в БД)процедура, которая найдет счета, где остатки не соответствуют операциям,
 и восстановливает правильные остатки"""

from db_utils import connect_to_db, execute_sql
from psycopg2 import sql

# Функция для обновления остатка счета
def update_account_saldo(conn, account_id, new_saldo):
    update_query = sql.SQL("""
        UPDATE ACCOUNTS
        SET SALDO = %s
        WHERE ID = %s;
    """) 
    execute_sql(conn, update_query, params=(new_saldo, account_id)) 

# Создаем подключение к БД
conn = connect_to_db()

if conn:
    # SQL-запрос для поиска счетов с несовпадающими остатками
    check_query = sql.SQL("""
        SELECT 
            a.ID AS account_id,
            a.SALDO AS current_saldo,
            COALESCE(SUM(CASE WHEN r.DT = 1 THEN -r.SUM ELSE r.SUM END), 0) AS calculated_saldo
        FROM ACCOUNTS a
        LEFT JOIN RECORDS r ON a.ID = r.ACC_REF
        GROUP BY a.ID, a.SALDO
        HAVING a.SALDO <> COALESCE(SUM(CASE WHEN r.DT = 1 THEN -r.SUM ELSE r.SUM END), 0)
    """)

    # Выполняем SQL-запрос
    mismatched_accounts = execute_sql(conn, check_query, fetch=True) 

    if mismatched_accounts:
        print("Обнаружены счета с несовпадающими остатками:")
        for row in mismatched_accounts:
            account_id = row[0]
            current_saldo = row[1]
            calculated_saldo = row[2]

            print(f"  ID счета: {account_id}")
            print(f"  Текущий остаток: {current_saldo}")
            print(f"  Рассчитанный остаток: {calculated_saldo}")

            update_account_saldo(conn, account_id, calculated_saldo)
            print("  Остаток обновлен!")
            print("-" * 30)
    else:
        print("Все остатки счетов соответствуют операциям.")
    
    conn.close()