import psycopg2 as pg


def create_table(connection: pg.connect, table_name, *columns):  # создает таблицы
    if isinstance(table_name, str):
        string_columns = ""
        for column in columns:
            if isinstance(column, str):
                string_columns += column + ", "
        string_columns = string_columns.strip(", ")
        with connection.cursor() as cursor:
            if string_columns > "":
                try:
                    cursor.execute(
                        f"CREATE TABLE IF NOT EXISTS {table_name} ({string_columns})"
                    )
                    return True
                except pg.DatabaseError as e:
                    print(f"Не удалось создать таблицу по причине: {e}")
    return False


def create_db_struct_vkinder(connection):
    table_list = [
        {
            "table_name": "target_users_vk",
            "parameters": (
                "id serial PRIMARY KEY",
                "id_vk integer UNIQUE NOT NULL",
                "login varchar(100)",
                "token varchar(100)",
                "first_name varchar(100)",
                "last_name varchar(100)",
                "sex integer",
                "city integer",
                "age integer",
                "activities varchar[]",
                "interests varchar[]",
                "movies varchar[]",
                "music varchar[]",
                "books varchar[]",
                "quotes varchar[]",
                "about varchar[]",
                "home_town varchar[]",
            ),
        },
        {
            "table_name": "users_vk",
            "parameters": (
                "id serial PRIMARY KEY",
                "id_vk integer UNIQUE NOT NULL",
                "first_name varchar(100)",
                "last_name varchar(100)",
                "sex integer",
                "city integer",
                "age integer",
                "activities varchar[]",
                "interests varchar[]",
                "movies varchar[]",
                "music varchar[]",
                "books varchar[]",
                "quotes varchar[]",
                "about varchar[]",
                "home_town varchar[]",
                "relation integer",
            ),
        },
        {
            "table_name": "grade_users",
            "parameters": (
                "id serial PRIMARY KEY",
                "target_users_id integer references target_users_vk(id)",
                "users_id integer references users_vk(id)",
                "points_auto integer",
                "points_user integer DEFAULT 0",
                "num_common_friends integer",
                "num_common_groups integer",
                "export_state boolean DEFAULT false",
                "UNIQUE (target_users_id, users_id)",
            ),
        },
    ]
    for table in table_list:
        with connection:
            create_table(connection, table.get("table_name"), *table.get("parameters"))


def drop_table(connection, table_names):
    drop_all_table_flag = True
    for table_name in table_names:
        with connection:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(f"DROP TABLE {table_name} CASCADE")
                except pg.DatabaseError:
                    drop_all_table_flag = False
    if drop_all_table_flag:
        print("Все данные были удалены")
        return False
    else:
        print("Не все данные удалены")
        return True


def get_id_vk_token_on_login(connection, login) -> (int, str):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "select id_vk, token from target_users_vk where login = %s", [login]
            )
            result_id_vk = cursor.fetchall()
            if result_id_vk:
                return result_id_vk[0]


def get_token_vk_on_login(connection, login) -> str:
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "select token from target_users_vk where login = %s", [login]
            )
            result_token_vk = cursor.fetchall()
            if result_token_vk:
                return result_token_vk[0][0]


def insert_update_user_vk(connection, table_name, id_vk, **kwargs):
    with connection.cursor() as cursor:
        columns_list = []
        values_list = []
        for column, value in kwargs.items():
            columns_list.append(column)
            values_list.append(value)
        columns_string = ", ".join(columns_list)
        s_string = len(values_list) * ", %s"
        columns_string_update = ", ".join(
            map(lambda column_func: f"{column_func} = EXCLUDED.{column_func}", columns_list)
        )
        # filer_columns_string = " ".join(
        #     map(lambda column: f"and (EXCLUDED.{column} is not null)", columns_list)
        # )
        sql_string = (
            f"INSERT INTO {table_name} (id_vk, {columns_string}) VALUES ({id_vk} {s_string}) "
            f"ON CONFLICT (id_vk) DO UPDATE SET "
            f"{columns_string_update} "
            # f"where 1 = 1 {filer_columns_string}"
            f"RETURNING *"
        )
        try:
            cursor.execute(sql_string, values_list)
            result = cursor.fetchall()
            if len(result) > 0:
                return result[0]
        except pg.DatabaseError as e:
            print(f'Ошибка базы данных: {e}')


def insert_update_grade_users(connection, target_users_id, users_id, **kwargs):
    with connection.cursor() as cursor:
        columns_list = []
        values_list = []
        for column, value in kwargs.items():
            columns_list.append(column)
            values_list.append(value)
        columns_string = ", ".join(columns_list)
        s_string = len(values_list) * ", %s"
        columns_string_update = ", ".join(
            map(lambda column_in_list: f"{column_in_list} = EXCLUDED.{column_in_list}", columns_list)
        )
        sql_string = (
            f"INSERT INTO grade_users (target_users_id, users_id, {columns_string}) "
            f"VALUES ({target_users_id}, {users_id} {s_string}) "
            f"ON CONFLICT (target_users_id, users_id) DO UPDATE SET "
            f"{columns_string_update} "
            f"RETURNING *"
        )
        # print(sql_string)
        cursor.execute(sql_string, values_list)
        result = cursor.fetchall()[0]
        return result


def get_grade_users(connection, target_id, users_vk_id, in_black=None):
    if in_black:
        sql_string_filter_in_black = " and (gr.points_user < 0)"
    else:
        sql_string_filter_in_black = " and (gr.points_user >= 0)"
    if isinstance(users_vk_id, (tuple, list, set)):
        list_to_result: bool = True
        sql_string_filter_users = "and (us.id_vk in (" + ", ".join(users_vk_id) + "))"
    elif users_vk_id == 0:
        list_to_result: bool = True
        sql_string_filter_users = ""
    else:
        list_to_result: bool = False
        sql_string_filter_users = f"and (us.id_vk = {users_vk_id})"

    sql_string = (
        f"select gr.users_id, gr.num_common_groups, gr.points_auto from  grade_users as gr "
        f"join users_vk as us on gr.users_id = us.id "
        f"where (gr.target_users_id = {target_id}) {sql_string_filter_users}"
        + sql_string_filter_in_black
    )
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_string)
            result = cursor.fetchall()
            if result:
                if list_to_result:
                    return result
                else:
                    return result[0]


def top_from_grade_users(connection, limit, user_id_bd, export_state=None):
    if isinstance(limit, int):
        with connection:
            with connection.cursor() as cursor:
                if export_state is not None:
                    sql_string_export = f"and gu.export_state = {export_state} "
                else:
                    sql_string_export = ""
                cursor.execute(
                    f"select gu.* from grade_users gu where gu.target_users_id = {user_id_bd} "
                    f"and gu.points_user >= 0 "
                    + sql_string_export
                    + f"order by points_auto DESC limit {limit}"
                )
                top = cursor.fetchall()
                return top


def get_user_of_id_vk(connection, table_name, id_vk, bd=False):
    with connection:
        with connection.cursor() as cursor:
            id_str = "" if bd else "_vk"
            cursor.execute(f"select * from {table_name} where id{id_str} = {id_vk}")
            result_user_vk = cursor.fetchall()
            if result_user_vk:
                return result_user_vk[0]


def get_users_of_id_vk(
    connection: pg.connect,
    table_name: str,
    ids_vk: (list, tuple, set),
    bd: bool = False,
) -> list:
    id_str = "" if bd else "_vk"
    ids_vk_string = ", ".join(map(str, ids_vk))
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"select * from {table_name} where id{id_str} in ({ids_vk_string})"
            )
            result_user_vk = cursor.fetchall()
            if result_user_vk:
                return result_user_vk


def count_in_table(connection, table_name):
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(f"select count(*) from {table_name}")
                return cursor.fetchall()[0][0]
            except pg.DatabaseError as e:
                print(f"Не существует такой таблицы: {e}")


def all_items_in_table(connection, table_name):
    with connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(f"select * from {table_name}")
                return cursor.fetchall()
            except pg.DatabaseError as e:
                print(f"Не существует такой таблицы: {e}")


def black_list(connection_db, target_id_bd) -> list:
    if input("Добавить в черный список пользователя из ТОП 100? да(у)/нет(n)").lower() in (
        "да",
        "yes",
        "y"
    ):
        top_list_user_id_bd = []
        for user_vk in top_from_grade_users(connection_db, 100, target_id_bd):
            top_list_user_id_bd.append(user_vk[2])
        top_list_users = dict()
        for number, user_id_bd in enumerate(top_list_user_id_bd, 1):
            user_vk = get_user_of_id_vk(connection_db, "users_vk", user_id_bd, True)
            top_list_users.update({f"{number}": user_id_bd})
            print(f"{number}:{user_vk[2]} {user_vk[3]} https://vk.com/id{user_vk[1]}")

        users_to_black_list = set(
            input(
                "Введите номера пользователей для добавление их в черный список через пробел "
            ).split(" ")
        )

        users_id_bd_set = set()
        for number in users_to_black_list:
            user_id_bd_to_list = top_list_users.get(number.strip())
            if user_id_bd_to_list:
                users_id_bd_set.add(user_id_bd_to_list)

        if len(users_id_bd_set) > 0:
            count_to_black = 0
            with connection_db:
                for user_id_bd in users_id_bd_set:
                    if insert_update_grade_users(
                        connection_db, target_id_bd, user_id_bd, points_user=-1
                    ):
                        count_to_black += 1
            print(f"В черный список добавили {count_to_black} пользователей")

    all_black_query = get_grade_users(connection_db, target_id_bd, 0, True)
    if all_black_query:
        all_users_id_bd_black = list(
            map(
                lambda user_in_query: user_in_query[0],
                get_grade_users(connection_db, target_id_bd, 0, True),
            )
        )
    else:
        all_users_id_bd_black = None

    if all_users_id_bd_black and len(all_users_id_bd_black) > 0:
        print("Черный список")
        for num, user in enumerate(
            get_users_of_id_vk(connection_db, "users_vk", all_users_id_bd_black, True),
            1,
        ):
            print(f"{num}:{user[2]} {user[3]}")
        return all_users_id_bd_black
    else:
        print("Черный список пуст")
