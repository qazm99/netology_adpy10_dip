import vk
import psycopg2 as pg
import requests
from getpass import getpass
import math
import json
import random

from datetime import datetime as dt

from db_engine import (
    insert_update_user_vk,
    insert_update_grade_users,
    create_db_struct_vkinder,
    drop_table,
    get_id_vk_token_on_login,
    get_user_of_id_vk,
    top_from_grade_users,
    get_grade_users,
    black_list,
)
from qazm import posintput, StatusBar, list_from_string
from settings import COUNT_IN_SEARCH, USER_FIX, DATA_BASE, VK_V, APPLICATION_ID
from vkapi import (
    get_best_photo,
    find_group_all_users_list_25,
    api_reuest,
)


class VKUser:
    def __init__(
        self,
        user_vk_id,
        user_bd_id=0,
        first_name="",
        last_name="",
        sex=0,
        city=0,
        age=0,
        activities="",
        interests="",
        movies="",
        music="",
        books="",
        quotes="",
        about="",
        home_town="",
    ):
        self.user_vk_id = user_vk_id
        self.user_bd_id = user_bd_id
        self.first_name = first_name
        self.last_name = last_name
        self.sex = sex
        self.city = city
        self.age = age
        self.activities = activities
        self.interests = interests
        self.movies = movies
        self.music = music
        self.books = books
        self.quotes = quotes
        self.about = about
        self.home_town = home_town


class SearchParametr:
    def __init__(
        self,
        q="",
        city=0,
        age_from=0,
        age_to=0,
        count=0,
        sex=0,
        has_photo=0,
        status=[0],
    ):
        self.q = q
        self.city = city
        self.age_from = age_from
        self.age_to = age_to
        self.count = count
        self.sex = sex
        self.has_photo = has_photo
        self.status = status

    def set_default(self):
        self.q = ""
        self.city = 0
        self.age_from = 0
        self.age_to = 0
        self.count = 0
        self.sex = 0
        self.has_photo = 0
        self.status = [
            0,
        ]


# list поиск количества пересечений в списках
def calc_common_param(in_list_first, in_list_second) -> int:
    if isinstance(in_list_first, (list, set)) and isinstance(
        in_list_second, (list, set)
    ):
        common_elements = set(in_list_first) & set(in_list_second)
        if len(common_elements) > 0:
            return len(common_elements)


# db vk_api пытаемся по логину получить доступ к API VK
def get_id_vk_vkapi_of_login(
    connection, current_login, vk_v, application_id
) -> (int, vk.API):
    vk_api = None
    vk_id = None
    current_user = get_id_vk_token_on_login(connection, current_login)
    # если нет такого логина в БД идем на сайт ВК
    test_session_vk_flag = False
    connect_to_site_flag = True
    if current_user:
        vk_id = current_user[0]
        # print('юзер есть')
        # test_token = f"{current_user[1]}"
        # print(f'test_token:{test_token}')
        # проверяем токен на валидность
        try:
            session_vk = vk.Session(f"{current_user[1]}")
            if vk.API(session=session_vk, timeout=60, v=vk_v).users.get():
                test_session_vk_flag = True
        except (vk.api.VkAPIError, vk.api.VkAuthError, requests.exceptions.ConnectionError) as e:
            if e.__str__().find("Errno 11001") > 0:
                print("Не могу подключиться к сайту, проверьте соединение")

            else:
                print("Не удалось подключиться к сайту")
            connect_to_site_flag = False
    # если токен не валидный и есть соединение с сайтом пробуем его получить через пароль
    if connect_to_site_flag:
        if not current_user or not test_session_vk_flag:
            try:
                # пробуем получить токен
                password = getpass(prompt="Введите пароль:")
                session_vk_login = vk.AuthSession(
                    application_id, current_login, password,
                )
                # логинимся через токен для его проверки
                session_vk = vk.Session(session_vk_login.access_token)
                vk_api = vk.API(session_vk, timeout=60, v=vk_v)
                user_target_dict = vk_api.users.get()[0]
                # сохраняем пользователя/ обновляем токен в БД
                user_in_bd = insert_update_user_vk(
                    connection,
                    "target_users_vk",
                    user_target_dict.get("id"),
                    login=current_login,
                    token=session_vk.access_token,
                )
                vk_id = user_in_bd[1]
            except vk.api.VkAuthError as e:
                if e.__str__().find("incorrect password") > 0:
                    print(f"Пароль не верный")
        else:  # логин имеется в бд и токен работает
            session_vk = vk.Session(f"{current_user[1]}")
            vk_api = vk.API(session_vk, timeout=60, v=vk_v)
    if vk_id and vk_api:
        return vk_id, vk_api


# db vk_api записываем данные текущего пользователя
def set_current_target_user(connection, vk_api) -> VKUser:
    # записываем все данные пользователя с ВК
    user_target_dict = None
    try:
        user_target_dict = vk_api.users.get(
            fields=[
                "city",
                "bdate",
                "sex",
                "relation",
                "status",
                "activities",
                "interests",
                "music",
                "movies",
                "personal",
                "relation",
                "common_count",
                "has_photo",
                "books",
                "quotes",
                "about",
                "home_town",
            ]
        )[0]
    except requests.exceptions.HTTPError as e:
        print(f"Не удалось получить данные пользователя: {e}")
    if user_target_dict:
        with connection:
            parametrs_user_dict = dict(
                first_name=user_target_dict.get("first_name"),
                last_name=user_target_dict.get("last_name"),
                age=(int(dt.now().year) - int(user_target_dict.get("bdate")[-4:])),
                sex=user_target_dict.get("sex"),
                city=user_target_dict.get("city").get("id")
                if user_target_dict.get("city")
                else None,
                activities=list_from_string(user_target_dict.get("activities")),
                interests=list_from_string(user_target_dict.get("interests")),
                movies=list_from_string(user_target_dict.get("movies")),
                music=list_from_string(user_target_dict.get("music")),
                books=list_from_string(user_target_dict.get("books")),
                quotes=list_from_string(user_target_dict.get("quotes")),
                about=list_from_string(user_target_dict.get("about")),
                home_town=list_from_string(user_target_dict.get("home_town")),
            )
            # фильтр пустых значений:
            param_for_delete_list = []
            for key, value in parametrs_user_dict.items():
                if value is None:
                    param_for_delete_list.append(key)
            for key in param_for_delete_list:
                parametrs_user_dict.pop(key)

            user_in_bd = list(
                insert_update_user_vk(
                    connection,
                    "target_users_vk",
                    user_target_dict.get("id"),
                    **parametrs_user_dict,
                )
            )

        if user_in_bd:
            params_for_update = dict()
            if user_in_bd[6] not in (1, 2):  # пол
                sex_exist = None
                while not sex_exist:
                    sex = input(
                        "Выберете ваш пол: мужской введите - м, женский введите - ж: "
                    ).lower()
                    sex_dict = {"m": 2, "м": 2, "ж": 1}
                    if sex_dict.get(sex):
                        user_in_bd[6] = sex_dict.get(sex)
                        params_for_update.update(sex=user_in_bd[6])
                        sex_exist = True

            if user_in_bd[7] is None:  # город
                while not user_in_bd[7]:
                    params_city = dict(
                        q=input("Введите наименование вашего города: "), country_id=1
                    )
                    city_list = api_reuest(vk_api, "database.getCities", **params_city)
                    if city_list:
                        if city_list.get("count") > 0:
                            city_list_find = city_list.get("items")
                            print("0-Ввести другой город")
                            for number, city in enumerate(city_list_find, 1):
                                city_string = f'{number}-{city.get("title")}'
                                if city.get("area"):
                                    city_string += f', {city.get("area")}'
                                if city.get("region"):
                                    city_string += f', {city.get("region")}'
                                print(city_string)
                            city_number = (
                                posintput(
                                    "Введите номер вашего города: ",
                                    0,
                                    len(city_list.get("items"),),
                                )
                                - 1
                            )
                            if city_number >= 0:
                                user_in_bd[7] = city_list_find[city_number].get("id")
                            params_for_update.update(city=user_in_bd[7])
                        else:
                            print("Не найдено подходящих городов, попробуйте снова")
                    else:
                        print("Поиск города не удался - попробуйте снова")

            if (user_in_bd[8] is None) or (user_in_bd[8] > 150):  # возраст
                user_in_bd[8] = posintput("Введите свой возраст: ", 10, 150)
                params_for_update.update(age=user_in_bd[8])

            if user_in_bd[9] is None:
                while not user_in_bd[9]:
                    user_in_bd[9] = list_from_string(
                        input("Введите через запятую вашу деятельность: ")
                    )
                    if user_in_bd[9]:
                        params_for_update.update(activities=user_in_bd[9])

            if user_in_bd[10] is None:
                while not user_in_bd[10]:
                    user_in_bd[10] = list_from_string(
                        input("Введите через запятую ваши интересы: ")
                    )
                    if user_in_bd[10]:
                        params_for_update.update(interests=user_in_bd[10])

            if user_in_bd[11] is None:
                while not user_in_bd[11]:
                    user_in_bd[11] = list_from_string(
                        input("Введите через запятую ваши любимые фильмы: ")
                    )
                    if user_in_bd[11]:
                        params_for_update.update(movies=user_in_bd[11])

            if user_in_bd[12] is None:
                while not user_in_bd[12]:
                    user_in_bd[12] = list_from_string(
                        input("Введите через запятую вашу любимую музыку: ")
                    )
                    if user_in_bd[12]:
                        params_for_update.update(music=user_in_bd[12])

            if user_in_bd[13] is None:
                while not user_in_bd[13]:
                    user_in_bd[13] = list_from_string(
                        input("Введите через запятую ваши любимые книги: ")
                    )
                    if user_in_bd[13]:
                        params_for_update.update(books=user_in_bd[13])

            if user_in_bd[14] is None:
                while not user_in_bd[14]:
                    user_in_bd[14] = list_from_string(
                        input("Введите через запятую ваши любимые цитаты: ")
                    )
                    if user_in_bd[14]:
                        params_for_update.update(quotes=user_in_bd[14])

            if user_in_bd[15] is None:
                while not user_in_bd[15]:
                    user_in_bd[15] = list_from_string(
                        input("Расскажите немного о себе, кратко через запятую: ")
                    )
                    if user_in_bd[15]:
                        params_for_update.update(about=user_in_bd[15])

            if user_in_bd[16] is None:
                while not user_in_bd[16]:
                    user_in_bd[16] = list_from_string(
                        input("Введите свои любимые или родные города через запятую: ")
                    )
                    if user_in_bd[16]:
                        params_for_update.update(home_town=user_in_bd[16])

            # Записываем то что дозаполнили вручную
            if len(params_for_update) > 0:
                with connection:
                    insert_update_user_vk(
                        connection,
                        "target_users_vk",
                        user_in_bd[1],
                        **params_for_update,
                    )

            return VKUser(
                user_in_bd[1],
                user_in_bd[0],
                user_in_bd[4],
                user_in_bd[5],
                user_in_bd[6],
                user_in_bd[7],
                user_in_bd[8],
                user_in_bd[9],
                user_in_bd[10],
                user_in_bd[11],
                user_in_bd[12],
                user_in_bd[13],
                user_in_bd[14],
                user_in_bd[15],
                user_in_bd[16],
            )


# db vk_api
def search_users_of_parametr(connection, vk_api, search_p, current_target_user):
    all_users_id_search = []
    count_all_iteration = len(search_p.status) * (
        search_p.age_to - search_p.age_from + 1
    )
    print("Поиск подходящих пользователей")
    status_bar_find = StatusBar(count_all_iteration)
    for current_status in search_p.status:
        count_errors = 0
        for current_age in range(search_p.age_from, search_p.age_to + 1):
            status_bar_find.plus(1)
            dict_search = dict(
                q=search_p.q,
                city=search_p.city,
                age_from=current_age,
                age_to=current_age,
                count=search_p.count,
                sex=search_p.sex,
                has_photo=search_p.has_photo,
                status=current_status,
                sort=random.choice((0, 1)),
                fields=[
                    "bdate",
                    "relation",
                    "city",
                    "common_count",
                    "sex",
                    "activities",
                    "interests",
                    "music",
                    "movies",
                    "books",
                    "quotes",
                    "about",
                    "home_town",
                ],
            )
            users_search = api_reuest(vk_api, "users.search", **dict_search)
            # счетчик ошибок, если на одном пользователе нет ответа, то выходим из цикла досрочно
            if not users_search:
                count_errors += 1
                if count_errors == 10:
                    break
                continue

            with connection:
                for user in users_search.get("items"):
                    # print(user)
                    try:
                        user_current_search = insert_update_user_vk(
                            connection,
                            "users_vk",
                            user.get("id"),
                            first_name=user.get("first_name"),
                            last_name=user.get("last_name"),
                            age=current_age,
                            city=search_p.city,
                            sex=user.get("sex"),
                            relation=current_status,
                            activities=list_from_string(user.get("activities")),
                            interests=list_from_string(user.get("interests")),
                            movies=list_from_string(user.get("movies")),
                            music=list_from_string(user.get("music")),
                            books=list_from_string(user.get("books")),
                            quotes=list_from_string(user.get("quotes")),
                            about=list_from_string(user.get("about")),
                            home_town=list_from_string(user.get("home_town")),
                        )
                        user_id_bd = user_current_search[0]
                        # print('user', user_current_search)
                        all_users_id_search.append(user_id_bd)
                        # if user_current_search[7]:
                        #     common_activities = set(current_target_user.activities) & set(user_current_search[7])
                        #     print(f'common_activities:{common_activities}')
                        common_activities = calc_common_param(
                            current_target_user.activities, user_current_search[7]
                        )
                        common_interests = calc_common_param(
                            current_target_user.interests, user_current_search[8]
                        )
                        common_movies = calc_common_param(
                            current_target_user.movies, user_current_search[9]
                        )
                        common_music = calc_common_param(
                            current_target_user.music, user_current_search[10]
                        )
                        common_books = calc_common_param(
                            current_target_user.books, user_current_search[11]
                        )
                        common_quotes = calc_common_param(
                            current_target_user.quotes, user_current_search[12]
                        )
                        common_about = calc_common_param(
                            current_target_user.about, user_current_search[13]
                        )
                        common_home_town = calc_common_param(
                            current_target_user.home_town, user_current_search[14]
                        )

                        points_relation = calc_points(current_status, {1: 5, 6: 15})
                        points_age = calc_points(
                            int(math.fabs(current_target_user.age - current_age)),
                            {0: 20, 1: 17, 2: 15, 3: 12, 4: 10, 5: 5},
                        )
                        points_activities = calc_points(
                            common_activities, {1: 10, 2: 20, 3: 30}
                        )
                        points_interests = calc_points(
                            common_interests, {1: 10, 2: 20, 3: 30}
                        )
                        points_movies = calc_points(
                            common_movies, {1: 10, 2: 13, 3: 15}
                        )
                        points_music = calc_points(common_music, {1: 20, 2: 25, 3: 30})
                        points_books = calc_points(common_books, {1: 10, 2: 15, 3: 20})
                        points_quotes = calc_points(
                            common_quotes, {1: 10, 2: 20, 3: 30}
                        )
                        points_about = calc_points(common_about, {1: 10, 2: 20, 3: 30})
                        points_home_town = calc_points(
                            common_home_town, {1: 20, 2: 25, 3: 30}
                        )
                        points_common_friends = calc_points(
                            user.get("common_count"), {1: 30, 2: 35, 3: 40}
                        )

                        points_auto = (
                            points_age
                            + points_relation
                            + points_activities
                            + points_interests
                            + points_movies
                            + points_music
                            + points_books
                            + points_quotes
                            + points_about
                            + points_home_town
                            + points_common_friends
                        )

                        insert_update_grade_users(
                            connection,
                            current_target_user.user_bd_id,
                            user_id_bd,
                            points_auto=points_auto,
                            num_common_friends=user.get("common_count"),
                        )

                    except TypeError as e:
                        print(f"Ошика типов: {e}")
    if all_users_id_search:
        return all_users_id_search


# local вычисление рейтинга для каждого параметра
def calc_points(common_count, dict_points):
    if isinstance(common_count, int) and isinstance(dict_points, dict):
        points = dict_points.get(common_count)
        if not points:
            if common_count == 0:
                return 0
            else:
                return max(dict_points.values())
        else:
            return points
    else:
        # print(common_count)
        # print(dict_points)
        return 0


# db vk_api поиск подходящих пользователей для текущего пользователя
def find_users_for_user(connection_db, vk_api, current_target_user):
    sex_dict = {1: 2, 2: 1}
    count_search_max = COUNT_IN_SEARCH
    q_string = ""
    search_p = SearchParametr(
        q_string,
        current_target_user.city,
        current_target_user.age - 5,
        current_target_user.age + 5,
        count_search_max,
        sex_dict.get(current_target_user.sex),
        1,
        [1, 6],
    )

    all_id_vk_search = search_users_of_parametr(
        connection_db, vk_api, search_p, current_target_user
    )
    return all_id_vk_search


# db vk_api
def calc_top_for_user(connection_db, vk_api, current_target_user):
    if vk_api and True:
        # определяем топ 99 пользователей
        top_100 = top_from_grade_users(
            connection_db, 99, current_target_user.user_bd_id
        )
        # первый пользователь - целевой
        top_100_ids_vk = [current_target_user.user_vk_id]
        for item in top_100:
            top_100_ids_vk.append(
                get_user_of_id_vk(connection_db, "users_vk", item[2], True)[1]
            )
        # забираем для них и текущего пользователя группы
        groups_top_100 = find_group_all_users_list_25(top_100_ids_vk, vk_api)
        groups_target_user = groups_top_100[0].get(current_target_user.user_vk_id)
        for num_user, user_groups in enumerate(groups_top_100[1:]):

            for user, groups_user in user_groups.items():
                common_groups = calc_common_param(groups_target_user, groups_user)
                if common_groups:
                    grade_in = get_grade_users(
                        connection_db, current_target_user.user_bd_id, user
                    )
                    if grade_in and not grade_in[1]:
                        points_group = calc_points(common_groups, {1: 20, 2: 25, 3: 30})
                        points_auto = grade_in[2] + points_group
                        insert_update_grade_users(
                            connection_db,
                            current_target_user.user_bd_id,
                            grade_in[0],
                            points_auto=points_auto,
                            num_common_groups=common_groups,
                        )


# db vk_api выгрузка в файл
def top_10_to_file_for_user(connection_db, vk_api, current_target_user):
    if True:
        dict_top10 = dict()
        current_top10 = top_from_grade_users(
            connection_db, 10, current_target_user.user_bd_id, False
        )
        users_id_bd_list = []
        status_bar_top10 = StatusBar(len(current_top10))
        print("Ищем самые лучшие фотографии пользователей")
        for item in current_top10:
            status_bar_top10.plus()
            users_id_bd_list.append(item[2])
            vk_id = get_user_of_id_vk(connection_db, "users_vk", item[2], bd=True)[1]
            url_user = f"https://vk.com/id{vk_id}"
            list_best_photo = get_best_photo(vk_api, vk_id)
            dict_top10.update({vk_id: {"url": url_user, "photos": list_best_photo}})
        filename = f'{current_target_user.user_vk_id}_{dt.now().strftime("%y%m%d_%H%M%S")}.json'
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(dict_top10, file)
        with connection_db:
            for user_id_bd in users_id_bd_list:
                insert_update_grade_users(
                    connection_db,
                    current_target_user.user_bd_id,
                    user_id_bd,
                    export_state=True,
                )
    return filename


# local
def main_run():
    """
        l – (login) – авторизация, необходима для начала поиска контактов;
        f – (find) – поиск подходящих пользователей;
        d – (delete) – удаление данных, данная команда удалит все данные поиска;
        i – (import) – выгрузка в файл подходящих пользователей;
        b - (black list) - добавление в черный список пользователей из топ 100;
        q - (quit) - команда, которая завершает выполнение программы;
        """
    print(
        "Вас приветствует программа VKinder!\n",
        "(Введите help, для просмотра списка поддерживаемых команд)\n",
    )
    try:
        connection_db = pg.connect(
            dbname=DATA_BASE.get("dbname"),
            user=DATA_BASE.get("user"),
            password=DATA_BASE.get("password"),
        )
    except pg.DatabaseError:
        connection_db = False
        print(
            "Не удалось подключиться к базе данных, проверьте настроки в файле settings.py"
        )

    vk_api = None
    db_clear = True
    current_target_user = None
    while connection_db:
        if db_clear:
            create_db_struct_vkinder(connection_db)
            db_clear = False
        user_command = input("Введите команду - ").lower().strip()
        if user_command == "d":
            drop_table(connection_db, ["grade_users", "target_users_vk", "users_vk"])
            vk_api = None
            db_clear = True
        elif user_command == "l":
            current_login = USER_FIX
            if not current_login:
                current_login = input("Введите логин ВК: ")
            target_id_vk_vk_api = get_id_vk_vkapi_of_login(
                connection_db, current_login, VK_V, APPLICATION_ID
            )

            if target_id_vk_vk_api:
                vk_api = target_id_vk_vk_api[1]

            if vk_api:
                target_user_in_bd = set_current_target_user(connection_db, vk_api)
                if target_user_in_bd:
                    current_target_user: VKUser = target_user_in_bd
                    print(
                        f"Добро пожаловать {current_target_user.first_name} "
                        f"{current_target_user.last_name}, "
                        f"ваша страница https://vk.com/id{current_target_user.user_vk_id}"
                    )
                else:
                    vk_api = None
                    print(
                        "Не удальось получить данные пользователя, попробуйте залогиниться еще раз"
                    )
            else:
                print("Не удалось авторизоваться на сайте VK")

        elif user_command in ("f", "i", "b"):
            if vk_api:
                if user_command == "f":
                    # поиск подходящих пользователей
                    amount_count_in_search = 0
                    if vk_api and current_target_user:
                        find_users = find_users_for_user(connection_db, vk_api, current_target_user)
                        if find_users:
                            amount_count_in_search = len(find_users)
                            # выборка пользователей для конкретного пользователя
                            calc_top_for_user(connection_db, vk_api, current_target_user)

                    print(
                        f"Поиск подходящих пользователей завершен, "
                        f"всего было найдено {amount_count_in_search} пользователей"
                    )

                elif user_command == "i":
                    # Запись в файл подходящих пользователей
                    current_file_name = top_10_to_file_for_user(
                        connection_db, vk_api, current_target_user
                    )

                    # Чтение из файла
                    if current_file_name:
                        print("Импорт топ 10 пользователей прошел успешно")
                        if input("Показать данные в файле?да(y)/нет(n): ").lower() in (
                            "да",
                            "yes",
                            "y",
                        ):
                            with open(current_file_name, "r", encoding="utf-8") as file:
                                for user_id, urls in json.load(file).items():
                                    user_vk = get_user_of_id_vk(
                                        connection_db, "users_vk", user_id
                                    )
                                    print(
                                        f"\n{user_vk[2]} {user_vk[3]} https://vk.com/id{user_vk[1]}"
                                    )
                                    if urls.get("photos"):
                                        for number, photo_url in enumerate(
                                            urls.get("photos"), 1
                                        ):
                                            print(f"Фото {number}: {photo_url}")
                elif user_command == "b":
                    # Добавление в черный список
                    black_list(connection_db, current_target_user.user_bd_id)

            else:
                print("Вам необходимо авторизоваться (команда - l)")
        elif user_command == "q":
            if input("Действительно хотите выйти из программы?да(у)/нет(n)").lower() in (
                "y",
                "yes",
                "да",
            ):
                break
        elif user_command in ("help", "h"):
            print(main_run.__doc__)
        else:
            print("Введите help для получения списка комманд")


if __name__ == "__main__":
    main_run()
