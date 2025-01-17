"""создание таблиц и их заполнение в БД PostgreSQL"""

from psycopg2 import sql
from db_utils import connect_to_db, execute_sql


# Создаем подключение к БД
conn = connect_to_db()

if conn:
    # SQL-запросы для создания таблиц
    create_table_clients_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS CLIENTS (
            ID SERIAL PRIMARY KEY,
            NAME VARCHAR(255) NOT NULL,
            PLACE_OF_BIRTH VARCHAR(255),
            DATE_OF_BIRTH DATE,
            ADDRESS VARCHAR(255),
            PASSPORT VARCHAR(255)
        );
    """)
    create_table_product_type_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS PRODUCT_TYPE (
            ID SERIAL PRIMARY KEY,
            NAME VARCHAR(255) NOT NULL,
            BEGIN_DATE DATE,
            END_DATE DATE,
            TARIF_REF INT
        );
    """)
    create_table_tarifs_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS TARIFS (
            ID SERIAL PRIMARY KEY,
            NAME VARCHAR(255) NOT NULL,
            COST DECIMAL(10, 2)
        );
    """)
    create_table_products_query = sql.SQL("""
       CREATE TABLE IF NOT EXISTS PRODUCTS (
            ID SERIAL PRIMARY KEY,
            PRODUCT_TYPE_ID INT,
            NAME VARCHAR(255) NOT NULL,
            CLIENT_REF INT,
            OPEN_DATE DATE,
            CLOSE_DATE DATE
       );
    """)
    create_table_accounts_query = sql.SQL("""
         CREATE TABLE IF NOT EXISTS ACCOUNTS (
            ID SERIAL PRIMARY KEY,
            NAME VARCHAR(255) NOT NULL,
            SALDO DECIMAL(10, 2),
            CLIENT_REF INT,
            OPEN_DATE DATE,
            CLOSE_DATE DATE,
            PRODUCT_REF INT,
            ACC_NUM VARCHAR(20)
        );
    """)
    create_table_records_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS RECORDS (
            ID SERIAL PRIMARY KEY,
            DT INT,
            ACC_REF INT,
            OPER_DATE DATE,
            SUM DECIMAL(10, 2)
        );
    """)
    
    # Выполняем SQL-запросы для создания таблиц
    execute_sql(conn, create_table_clients_query)
    execute_sql(conn, create_table_product_type_query)
    execute_sql(conn, create_table_tarifs_query)
    execute_sql(conn, create_table_products_query)
    execute_sql(conn, create_table_accounts_query)
    execute_sql(conn, create_table_records_query)
    
        # SQL-запросы для добавления внешних ключей
    add_fk_products_product_type = sql.SQL("""
        ALTER TABLE PRODUCTS
        ADD CONSTRAINT fk_products_product_type
        FOREIGN KEY (PRODUCT_TYPE_ID) REFERENCES PRODUCT_TYPE(ID);
    """)
    
    add_fk_products_client = sql.SQL("""
        ALTER TABLE PRODUCTS
        ADD CONSTRAINT fk_products_client
        FOREIGN KEY (CLIENT_REF) REFERENCES CLIENTS(ID);
    """)
    
    add_fk_accounts_product = sql.SQL("""
        ALTER TABLE ACCOUNTS
        ADD CONSTRAINT fk_accounts_product
        FOREIGN KEY (PRODUCT_REF) REFERENCES PRODUCTS(ID);
    """)
    
    add_fk_accounts_client = sql.SQL("""
        ALTER TABLE ACCOUNTS
        ADD CONSTRAINT fk_accounts_client
        FOREIGN KEY (CLIENT_REF) REFERENCES CLIENTS(ID);
    """)
    
    add_fk_records_accounts = sql.SQL("""
        ALTER TABLE RECORDS
        ADD CONSTRAINT fk_records_accounts
        FOREIGN KEY (ACC_REF) REFERENCES ACCOUNTS(ID);
    """)
    
    add_fk_product_type_tarifs = sql.SQL("""
        ALTER TABLE PRODUCT_TYPE
        ADD CONSTRAINT fk_product_type_tarifs
        FOREIGN KEY (TARIF_REF) REFERENCES TARIFS(ID);
    """)
    
    # Выполняем SQL-запросы для добавления внешних ключей
    execute_sql(conn, add_fk_products_product_type)
    execute_sql(conn, add_fk_products_client)
    execute_sql(conn, add_fk_accounts_product)
    execute_sql(conn, add_fk_accounts_client)
    execute_sql(conn, add_fk_records_accounts)
    execute_sql(conn, add_fk_product_type_tarifs)

     # SQL-запросы для добавления комментариев к столбцам
    comment_clients_id = sql.SQL("""
        COMMENT ON COLUMN CLIENTS.ID IS 'Уникальный идентификатор клиента';
    """)
    comment_clients_name = sql.SQL("""
        COMMENT ON COLUMN CLIENTS.NAME IS 'ФИО клиента';
    """)
    comment_clients_place_of_birth = sql.SQL("""
        COMMENT ON COLUMN CLIENTS.PLACE_OF_BIRTH IS 'Место рождения клиента';
    """)
    comment_clients_date_of_birth = sql.SQL("""
        COMMENT ON COLUMN CLIENTS.DATE_OF_BIRTH IS 'Дата рождения клиента';
    """)
    comment_clients_address = sql.SQL("""
        COMMENT ON COLUMN CLIENTS.ADDRESS IS 'Адрес проживания клиента';
    """)
    comment_clients_passport = sql.SQL("""
        COMMENT ON COLUMN CLIENTS.PASSPORT IS 'Паспортные данные клиента';
    """)
    
    comment_product_type_id = sql.SQL("""
       COMMENT ON COLUMN PRODUCT_TYPE.ID IS 'Уникальный идентификатор типа продукта';
    """)
    comment_product_type_name = sql.SQL("""
        COMMENT ON COLUMN PRODUCT_TYPE.NAME IS 'Наименование типа продукта';
    """)
    comment_product_type_begin_date = sql.SQL("""
        COMMENT ON COLUMN PRODUCT_TYPE.BEGIN_DATE IS 'Дата начала действия типа продукта';
    """)
    comment_product_type_end_date = sql.SQL("""
         COMMENT ON COLUMN PRODUCT_TYPE.END_DATE IS 'Дата окончания действия типа продукта';
    """)
    comment_product_type_tarif_ref = sql.SQL("""
        COMMENT ON COLUMN PRODUCT_TYPE.TARIF_REF IS 'Ссылка на тариф';
    """)
    
    comment_tarifs_id = sql.SQL("""
        COMMENT ON COLUMN TARIFS.ID IS 'Уникальный идентификатор тарифа';
    """)
    comment_tarifs_name = sql.SQL("""
        COMMENT ON COLUMN TARIFS.NAME IS 'Наименование тарифа';
    """)
    comment_tarifs_cost = sql.SQL("""
        COMMENT ON COLUMN TARIFS.COST IS 'Стоимость тарифа';
    """)
    
    comment_products_id = sql.SQL("""
       COMMENT ON COLUMN PRODUCTS.ID IS 'Уникальный идентификатор продукта';
    """)
    comment_products_product_type_id = sql.SQL("""
       COMMENT ON COLUMN PRODUCTS.PRODUCT_TYPE_ID IS 'Ссылка на тип продукта';
    """)
    comment_products_name = sql.SQL("""
       COMMENT ON COLUMN PRODUCTS.NAME IS 'Наименование продукта';
    """)
    comment_products_client_ref = sql.SQL("""
       COMMENT ON COLUMN PRODUCTS.CLIENT_REF IS 'Ссылка на клиента';
    """)
    comment_products_open_date = sql.SQL("""
        COMMENT ON COLUMN PRODUCTS.OPEN_DATE IS 'Дата открытия продукта';
    """)
    comment_products_close_date = sql.SQL("""
        COMMENT ON COLUMN PRODUCTS.CLOSE_DATE IS 'Дата закрытия продукта';
    """)
    
    comment_accounts_id = sql.SQL("""
        COMMENT ON COLUMN ACCOUNTS.ID IS 'Уникальный идентификатор счета';
    """)
    comment_accounts_name = sql.SQL("""
        COMMENT ON COLUMN ACCOUNTS.NAME IS 'Наименование счета';
    """)
    comment_accounts_saldo = sql.SQL("""
        COMMENT ON COLUMN ACCOUNTS.SALDO IS 'Остаток по счету';
    """)
    comment_accounts_client_ref = sql.SQL("""
        COMMENT ON COLUMN ACCOUNTS.CLIENT_REF IS 'Ссылка на клиента';
    """)
    comment_accounts_open_date = sql.SQL("""
        COMMENT ON COLUMN ACCOUNTS.OPEN_DATE IS 'Дата открытия счета';
    """)
    comment_accounts_close_date = sql.SQL("""
        COMMENT ON COLUMN ACCOUNTS.CLOSE_DATE IS 'Дата закрытия счета';
    """)
    comment_accounts_product_ref = sql.SQL("""
        COMMENT ON COLUMN ACCOUNTS.PRODUCT_REF IS 'Ссылка на продукт';
    """)
    comment_accounts_acc_num = sql.SQL("""
        COMMENT ON COLUMN ACCOUNTS.ACC_NUM IS 'Номер счета';
    """)
    
    comment_records_id = sql.SQL("""
        COMMENT ON COLUMN RECORDS.ID IS 'Уникальный идентификатор операции';
    """)
    comment_records_dt = sql.SQL("""
        COMMENT ON COLUMN RECORDS.DT IS 'Признак дебетования счета (1 - дебет, 0 - кредит)';
    """)
    comment_records_acc_ref = sql.SQL("""
        COMMENT ON COLUMN RECORDS.ACC_REF IS 'Ссылка на счет';
    """)
    comment_records_oper_date = sql.SQL("""
        COMMENT ON COLUMN RECORDS.OPER_DATE IS 'Дата операции';
    """)
    comment_records_sum = sql.SQL("""
        COMMENT ON COLUMN RECORDS.SUM IS 'Сумма операции';
    """)

    # Выполняем SQL-запросы для добавления комментариев
    execute_sql(conn, comment_clients_id)
    execute_sql(conn, comment_clients_name)
    execute_sql(conn, comment_clients_place_of_birth)
    execute_sql(conn, comment_clients_date_of_birth)
    execute_sql(conn, comment_clients_address)
    execute_sql(conn, comment_clients_passport)
    
    execute_sql(conn, comment_product_type_id)
    execute_sql(conn, comment_product_type_name)
    execute_sql(conn, comment_product_type_begin_date)
    execute_sql(conn, comment_product_type_end_date)
    execute_sql(conn, comment_product_type_tarif_ref)
    
    execute_sql(conn, comment_tarifs_id)
    execute_sql(conn, comment_tarifs_name)
    execute_sql(conn, comment_tarifs_cost)
    
    execute_sql(conn, comment_products_id)
    execute_sql(conn, comment_products_product_type_id)
    execute_sql(conn, comment_products_name)
    execute_sql(conn, comment_products_client_ref)
    execute_sql(conn, comment_products_open_date)
    execute_sql(conn, comment_products_close_date)
    
    execute_sql(conn, comment_accounts_id)
    execute_sql(conn, comment_accounts_name)
    execute_sql(conn, comment_accounts_saldo)
    execute_sql(conn, comment_accounts_client_ref)
    execute_sql(conn, comment_accounts_open_date)
    execute_sql(conn, comment_accounts_close_date)
    execute_sql(conn, comment_accounts_product_ref)
    execute_sql(conn, comment_accounts_acc_num)
    
    execute_sql(conn, comment_records_id)
    execute_sql(conn, comment_records_dt)
    execute_sql(conn, comment_records_acc_ref)
    execute_sql(conn, comment_records_oper_date)
    execute_sql(conn, comment_records_sum)
    
    # SQL-запросы для вставки данных
    insert_data_query = sql.SQL("""
        INSERT INTO TARIFS (ID, NAME, COST) VALUES
        (1, 'Тариф за выдачу кредита', 10),
        (2, 'Тариф за открытие счета', 10),
        (3, 'Тариф за обслуживание карты', 10);

        INSERT INTO PRODUCT_TYPE (ID, NAME, BEGIN_DATE, END_DATE, TARIF_REF) VALUES
        (1, 'КРЕДИТ', TO_DATE('01.01.2018','DD.MM.YYYY'), NULL, 1),
        (2, 'ДЕПОЗИТ', TO_DATE('01.01.2018','DD.MM.YYYY'), NULL, 2),
        (3, 'КАРТА', TO_DATE('01.01.2018','DD.MM.YYYY'), NULL, 3);

        INSERT INTO CLIENTS (ID, NAME, PLACE_OF_BIRTH, DATE_OF_BIRTH, ADDRESS, PASSPORT) VALUES
        (1, 'Сидоров Иван Петрович', 'Россия, Московская облать, г. Пушкин', TO_DATE('01.01.2001','DD.MM.YYYY'), 'Россия, Московская облать, г. Пушкин, ул. Грибоедова, д. 5', '2222 555555, выдан ОВД г. Пушкин, 10.01.2015'),
        (2, 'Иванов Петр Сидорович', 'Россия, Московская облать, г. Клин', TO_DATE('01.01.2001','DD.MM.YYYY'), 'Россия, Московская облать, г. Клин, ул. Мясникова, д. 3', '4444 666666, выдан ОВД г. Клин, 10.01.2015'),
        (3, 'Петров Сиодр Иванович', 'Россия, Московская облать, г. Балашиха', TO_DATE('01.01.2001','DD.MM.YYYY'), 'Россия, Московская облать, г. Балашиха, ул. Пушкина, д. 7', '4444 666666, выдан ОВД г. Клин, 10.01.2015');

        INSERT INTO PRODUCTS (ID, PRODUCT_TYPE_ID, NAME, CLIENT_REF, OPEN_DATE, CLOSE_DATE) VALUES
        (1, 1, 'Кредитный договор с Сидоровым И.П.', 1, TO_DATE('01.06.2015','DD.MM.YYYY'), NULL),
        (2, 2, 'Депозитный договор с Ивановым П.С.', 2, TO_DATE('01.08.2017','DD.MM.YYYY'), NULL),
        (3, 3, 'Карточный договор с Петровым С.И.', 3, TO_DATE('01.08.2017','DD.MM.YYYY'), NULL);

        INSERT INTO ACCOUNTS (ID, NAME, SALDO, CLIENT_REF, OPEN_DATE, CLOSE_DATE, PRODUCT_REF, ACC_NUM) VALUES
        (1, 'Кредитный счет для Сидоровым И.П.', -2000, 1, TO_DATE('01.06.2015','DD.MM.YYYY'), NULL, 1, '45502810401020000022'),
        (2, 'Депозитный счет для Ивановым П.С.', 6000, 2, TO_DATE('01.08.2017','DD.MM.YYYY'), NULL, 2, '42301810400000000001'),
        (3, 'Карточный счет для Петровым С.И.', 8000, 3, TO_DATE('01.08.2017','DD.MM.YYYY'), NULL, 3, '40817810700000000001');

        INSERT INTO RECORDS (ID, DT, ACC_REF, OPER_DATE, SUM) VALUES
        (1, 1, 1, TO_DATE('01.06.2015','DD.MM.YYYY'), 5000),
        (2, 0, 1, TO_DATE('01.07.2015','DD.MM.YYYY'), 1000),
        (3, 0, 1, TO_DATE('01.08.2015','DD.MM.YYYY'), 2000),
        (4, 0, 1, TO_DATE('01.09.2015','DD.MM.YYYY'), 3000),
        (5, 1, 1, TO_DATE('01.10.2015','DD.MM.YYYY'), 5000),
        (6, 0, 1, TO_DATE('01.10.2015','DD.MM.YYYY'), 3000),
        (7, 0, 2, TO_DATE('01.08.2017','DD.MM.YYYY'), 10000),
        (8, 1, 2, TO_DATE('05.08.2017','DD.MM.YYYY'), 1000),
        (9, 1, 2, TO_DATE('21.09.2017','DD.MM.YYYY'), 2000),
        (10, 1, 2, TO_DATE('24.10.2017','DD.MM.YYYY'), 5000),
        (11, 0, 2, TO_DATE('26.11.2017','DD.MM.YYYY'), 6000),
        (12, 0, 3, TO_DATE('08.09.2017','DD.MM.YYYY'), 120000),
        (13, 1, 3, TO_DATE('05.10.2017','DD.MM.YYYY'), 1000),
        (14, 1, 3, TO_DATE('21.10.2017','DD.MM.YYYY'), 2000),
        (15, 1, 3, TO_DATE('24.10.2017','DD.MM.YYYY'), 5000);
    """)
    
    # Выполняем SQL-запрос для вставки данных
    execute_sql(conn, insert_data_query)
    
    conn.close()