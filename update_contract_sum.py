"""
В модель данных добавим сумму договора по продукту.
Заполним поле для всех продуктов суммой максимальной дебетовой операции по счету для продукта типа «КРЕДИТ»,
и суммой максимальной кредитовой операции по счету продукта для продукта типа «ДЕПОЗИТ» или «КАРТА».
"""
from db_utils import connect_to_db, execute_sql
from psycopg2 import sql

# Создаем подключение к БД
conn = connect_to_db()

if conn:
    # SQL-запросы для добавления колонки CONTRACT_SUM
    add_column_products_contract_sum = sql.SQL("""
        ALTER TABLE PRODUCTS
        ADD COLUMN IF NOT EXISTS CONTRACT_SUM DECIMAL(10, 2);
    """)
    # Выполняем SQL-запрос для добавления колонки
    execute_sql(conn, add_column_products_contract_sum)
    
    # SQL-запрос для добавления комментария к столбцу CONTRACT_SUM
    comment_products_contract_sum = sql.SQL("""
        COMMENT ON COLUMN PRODUCTS.CONTRACT_SUM IS 'Сумма договора';
    """)
    # Выполняем SQL-запрос для добавления комментария
    execute_sql(conn, comment_products_contract_sum)
    
    
    # Функция для обновления суммы договора продукта
    def update_contract_sum(conn, product_id, contract_sum):
        update_query = sql.SQL("""
            UPDATE PRODUCTS
            SET CONTRACT_SUM = %s
            WHERE ID = %s;
        """) # <-ЗДЕСЬ ИЗМЕНЕНИЯ: (добавили параметры)
        execute_sql(conn, update_query, params=(contract_sum, product_id)) 
    
    # SQL-запрос для получения максимальной суммы операций по каждому продукту
    select_query = sql.SQL("""
        SELECT
            p.ID AS product_id,
            pt.NAME AS product_type_name,
            COALESCE(
                CASE
                    WHEN pt.NAME = 'КРЕДИТ' THEN (
                        SELECT MAX(r.SUM)
                        FROM ACCOUNTS a
                        JOIN RECORDS r ON a.ID = r.ACC_REF
                        WHERE a.PRODUCT_REF = p.ID AND r.DT = 1
                    )
                    ELSE (
                        SELECT MAX(r.SUM)
                        FROM ACCOUNTS a
                        JOIN RECORDS r ON a.ID = r.ACC_REF
                        WHERE a.PRODUCT_REF = p.ID AND r.DT = 0
                    )
                END, 0
            ) AS max_operation_sum
        FROM PRODUCTS p
        JOIN PRODUCT_TYPE pt ON p.PRODUCT_TYPE_ID = pt.ID
    """)
    
    # Выполняем SQL-запрос
    products_info = execute_sql(conn, select_query, fetch=True)
    
    if products_info:
        print("Обновление суммы договора для продуктов:")
        for row in products_info:
            product_id = row[0]
            product_type_name = row[1]
            max_operation_sum = row[2]
            
            print(f"  ID продукта: {product_id}, Тип продукта: {product_type_name}, Максимальная сумма операции: {max_operation_sum}")
            update_contract_sum(conn, product_id, max_operation_sum)
            print("  Сумма договора обновлена!")
            print("-" * 30)
    else:
        print("Нет данных для обновления суммы договора.")
    
    conn.close()