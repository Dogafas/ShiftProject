"""Выборка, которая содержит средние движения по счетам в рамках одного дня в разрезе типа продукта"""
import psycopg2
from db_utils import connect_to_db
from psycopg2 import sql

# Функция для выполнения SQL-запроса
def execute_sql(conn, query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        results = cur.fetchall()
        return results
    except psycopg2.Error as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None
    finally:
        cur.close()

# Основной блок
def main():
    conn = connect_to_db()  # Создаем подключение к БД
    if conn:
        try:
            # SQL-запрос для формирования отчета о средних движениях
            average_movements_query = sql.SQL("""
                SELECT
                    pt.NAME AS product_type,
                    AVG(daily_movements.total_sum) AS average_daily_movement
                FROM
                    PRODUCT_TYPE pt
                JOIN
                    PRODUCTS p ON pt.ID = p.PRODUCT_TYPE_ID
                JOIN
                    ACCOUNTS a ON p.ID = a.PRODUCT_REF
                JOIN (
                    SELECT
                        ACC_REF,
                        OPER_DATE,
                        SUM(CASE WHEN DT = 1 THEN -SUM ELSE SUM END) AS total_sum
                    FROM
                        RECORDS
                    GROUP BY
                        ACC_REF, OPER_DATE
                ) AS daily_movements ON a.ID = daily_movements.ACC_REF
                GROUP BY
                    pt.NAME
                ORDER BY
                    pt.NAME;
            """)

            # Выполняем SQL-запрос
            movements_results = execute_sql(conn, average_movements_query)

            if movements_results:
                # Выводим результаты
                print("Отчет по средним движениям по счетам в разрезе типа продукта:")
                for row in movements_results:
                    print(f"  Тип продукта: {row[0]}")
                    print(f"  Среднее движение в день: {row[1]:.2f}")
                    print("-" * 40)
            else:
                print("Нет данных, удовлетворяющих условиям запроса.")
        finally:
            conn.close()

if __name__ == "__main__":
    main()
