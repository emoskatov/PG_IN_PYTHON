import psycopg2
import os

PASSWORD = os.environ.get(
    "BD_PASSWORD")  # Для обеспечения конфиденциальности пароль от базы данных указан через переменную окружения


def create_db(connect):
    with connect.cursor() as cur:
        ''' Создание таблиц в базе данных 
        Для тестирования или если возникают ошибку из-за того что такие таблицы есть и заполнены данными, происходит 
        удаление таблиц каждый раз при выполнении функции
        Если ошибок нет, данную строку необходимо закомментировать, так как это создает дополнительную нагрузку на 
        файловую систему постоянным перезаписыванием.
        В самом запросе стоит проверка на существование таблицы и если она есть, пропускает без ошибок.
        '''
        cur.execute("drop table IF EXISTS client,client_phone CASCADE;")

        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS client(
                id serial PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL UNIQUE CHECK (email LIKE '%@%.%')
                );
            """
        )
        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS client_phone(
                id serial PRIMARY KEY,
                id_client integer REFERENCES client(id),
                phone_number varchar(16) NOT NULL UNIQUE CHECK (phone_number LIKE '+7(___)___-__-__')
                );
            """
        )
        connect.commit()


def add_client(connect, first_name, last_name, email, phones=None):
    with connect.cursor() as cur:
        cur.execute(
            """
                INSERT INTO
                    client (first_name, last_name, email)
                VALUES
                    (%s, %s, %s)
                returning id;             
            """,
            (first_name, last_name, email)
        )

        client_id = cur.fetchone()[0]

    if phones is not None:
        add_phone(connect=connect, client_id=client_id, phones=phones)


def add_phone(connect, client_id, phones):
    if type(phones) is list:
        for phone in phones:
            with connect.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO
                            client_phone (id_client, phone_number)
                        VALUES
                            (%s, %s);             
                    """,
                    (client_id, phone)
                )
    else:
        with connect.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO
                        client_phone (id_client, phone_number)
                    VALUES
                        (%s, %s);             
                """,
                (client_id, phones)
            )


def change_client(connect, client_id, first_name=None, last_name=None, email=None, phones=None):
    with connect.cursor() as cur:
        if first_name is not None:
            cur.execute(
                """
                    UPDATE
                        client
                    SET
                        first_name = %s
                    WHERE
                        id = %s;
                """,
                (first_name, client_id)
            )
        if last_name is not None:
            cur.execute(
                """
                    UPDATE
                        client
                    SET
                        last_name = %s
                    WHERE
                        id = %s;
                """,
                (last_name, client_id)
            )
        if email is not None:
            cur.execute(
                """
                    UPDATE
                        client
                    SET
                        email = %s
                    WHERE
                        id = %s;
                """,
                (email, client_id)
            )

        connect.commit()

    if phones is not None:
        add_phone(connect=connect, client_id=client_id, phones=phones)


def delete_phone(connect, client_id, phone):
    with connect.cursor() as cur:
        cur.execute(
            """
                DELETE FROM
                    client_phone
                WHERE
                    phone_number = %s
                    AND id_client = %s;
            """,
            (phone, client_id)
        )
        connect.commit()


def delete_client(connect, client_id):
    with connect.cursor() as cur:
        cur.execute(
            """
                DELETE FROM
                    client_phone
                WHERE
                    id_client = %s;
            """,
            (client_id,)
        )

        cur.execute(
            """
                DELETE FROM
                    client 
                WHERE
                    id = %s;
            """,
            (client_id,)
        )
        connect.commit()


def find_client(connect, first_name=None, last_name=None, email=None, phone=None):
    with connect.cursor() as cur:
        if first_name is not None:
            cur.execute(
                """
                    SELECT
                        c.first_name,
                        c.last_name,
                        c.email,
                        cp.phone_number
                    FROM
                        client AS c
                        JOIN client_phone AS cp ON c.id = cp.id_client
                    WHERE
                        c.first_name = %s;
                """,
                (first_name,)
            )
        elif last_name is not None:
            cur.execute(
                """
                    SELECT
                        c.first_name,
                        c.last_name,
                        c.email,
                        cp.phone_number
                    FROM
                        client AS c
                        JOIN client_phone AS cp ON c.id = cp.id_client
                    WHERE
                        c.last_name = %s;
                """,
                (last_name,)
            )
        elif email is not None:
            cur.execute(
                """
                    SELECT
                        c.first_name,
                        c.last_name,
                        c.email,
                        cp.phone_number
                    FROM
                        client AS c
                        JOIN client_phone AS cp ON c.id = cp.id_client
                    WHERE
                        c.email = %s;
                """,
                (email,)
            )
        elif phone is not None:
            cur.execute(
                """
                    SELECT
                        c.first_name,
                        c.last_name,
                        c.email,
                        cp.phone_number
                    FROM
                        client AS c
                        JOIN client_phone AS cp ON c.id = cp.id_client
                    WHERE
                        cp.phone_number = %s;
                """,
                (phone,)
            )
        else:
            print("Вы не ввели никаких данных")
            return

        answer = cur.fetchall()
        if len(answer) == 0:
            print("Ничего не нашлось, проверьте правильность вводимых данных")
        else:
            print(*answer[0])


with psycopg2.connect(database="clients_db", user="postgres",
                      password=PASSWORD) as connection:  # Пароль от базы данных с переменной окружения
    create_db(connect=connection)  # вызывайте функции здесь
    add_client(connection, "Ivan", "Ivanov", "ivan@test.ru")  # Добавление пользователя без номера телефона
    add_client(connection, "Ivan", "Sidorov", "Sidorov@test.ru",
            
               "+7(999)888-77-66")  # Добавление пользователя с номером телефона
    add_client(connection, "Sergey", "Sergeev", "Sergey@mail.ru", "+7(999)800-00-00")
    add_phone(connection, 1, "+7(888)999-33-11")  # Добавление одного номера телефона для существующего клиента
    add_phone(connection, 2, ["+7(000)000-11-11", "+7(000)000-11-22",
                              "+7(000)000-22-22"])  # Добавление нескольких номеров для второго клиента, итого 4
    change_client(connection, 2, first_name="Иван")  # Изменение для клиента 2 Имени
    change_client(connection, 1, last_name="Иванов")  # Изменение для клиента 1 Фамилии
    change_client(connection, 1, email="ivanov@test.ru")  # Изменение для клиента email
    delete_phone(connection, 2, "+7(000)000-22-22")  # Удаление номера телефона
    delete_client(connection, 2)  # Удаление клиента с очисткой также таблицы номеров
    find_client(connection, first_name="Sergey")  # Поиск клиента по имени
    find_client(connection, last_name="Sergeev")  # Поиск клиента по Фамилии
    find_client(connection, email="ivanov@test.ru")  # Поиск клиента по имени
    find_client(connection, phone="+7(888)999-33-11")  # Поиск клиента по телефону
    find_client(connection, last_name="Sidorov")  # Поиск по удаленному клиенту, ожидаемо не должен выдать результатов
    find_client(connection)  # Поиск по пустым данным, должен вернуть только текст

connection.close()
