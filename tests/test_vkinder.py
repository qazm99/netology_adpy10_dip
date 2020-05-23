import unittest

import db_engine
import vkapi
import vkinder
import vk

from fixtures.settings_test import DATA_BASE, USER_FIX, VK_V, APPLICATION_ID, USER_PASS
from fixtures.fix_data import (
    struct_table_test,
    test_target_user,
    test_user1,
    test_grade1,
)
import psycopg2 as pg
from unittest.mock import patch


class TestCommon(unittest.TestCase):
    def test_calc_common_param(self):
        list_first = ["one", "two", 3, 4]
        list_second = ["two", 4]
        list_wrong = [6, "seven", 8, "nine"]
        result_positive = vkinder.calc_common_param(list_first, list_second)
        self.assertEqual(result_positive, 2)
        result_positive = vkinder.calc_common_param(list_first, list_first)
        self.assertEqual(result_positive, len(list_first))
        result_negative = vkinder.calc_common_param(list_first, list_wrong)
        self.assertIsNone(result_negative)

    def test_calc_points(self):
        dict_points = {10: 100, 20: 90, 30: 80, 40: 70}
        result = vkinder.calc_points(30, dict_points)
        self.assertEqual(result, dict_points.get(30))
        result = vkinder.calc_points(50, dict_points)
        self.assertEqual(result, max(dict_points.values()))
        result = vkinder.calc_points(0, dict_points)
        self.assertEqual(result, 0)


class TestDB(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.connection_db = pg.connect(
            dbname=DATA_BASE.get("dbname"),
            user=DATA_BASE.get("user"),
            password=DATA_BASE.get("password"),
        )

    def setUp(self) -> None:
        vkinder.drop_table(
            self.connection_db, ["grade_users", "target_users_vk", "users_vk"]
        )
        vkinder.create_db_struct_vkinder(self.connection_db)

    def test_create_drop_table(self):
        db_engine.drop_table(self.connection_db, ("test_table_vk",))
        with self.connection_db:
            with self.connection_db.cursor() as cursor:
                cursor.execute(
                    "select exists (select from pg_tables where tablename = 'test_table_vk')"
                )
                result_drop = cursor.fetchall()[0][0]
                print(f"result_drop1 {result_drop}")
        self.assertFalse(result_drop)
        db_engine.create_table(
            self.connection_db,
            struct_table_test.get("table_name"),
            *struct_table_test.get("parameters"),
        )
        with self.connection_db:
            with self.connection_db.cursor() as cursor:
                cursor.execute(
                    "select exists (select from pg_tables where tablename = 'test_table_vk')"
                )
                result_create = cursor.fetchall()[0][0]
                print(f"result_create1 {result_create}")
        self.assertTrue(result_create)
        db_engine.drop_table(self.connection_db, ("test_table_vk",))
        with self.connection_db:
            with self.connection_db.cursor() as cursor:
                cursor.execute(
                    "select exists (select from pg_tables where tablename = 'test_table_vk')"
                )
                result_drop = cursor.fetchall()[0][0]
                print(f"result_drop2 {result_drop}")
        self.assertFalse(result_drop)

    def test_get_id_vk_token_on_login(self):
        sql_string = (
            f"INSERT INTO target_users_vk (id_vk, login, token) VALUES ({test_target_user.get('id_vk')}, %s, %s) "
            f"ON CONFLICT (id_vk) DO UPDATE SET "
            f"login = EXCLUDED.login , token = EXCLUDED.token "
            f"RETURNING *"
        )
        with self.connection_db:
            with self.connection_db.cursor() as cursor:
                cursor.execute(
                    sql_string,
                    [test_target_user.get("login"), test_target_user.get("token")],
                )

        result = db_engine.get_id_vk_token_on_login(
            self.connection_db, test_target_user.get("login")
        )
        self.assertEqual(result[0], test_target_user.get("id_vk"))
        self.assertEqual(result[1], test_target_user.get("token"))

    def test_get_token_vk_on_login(self):
        id_vk = test_target_user.get("id_vk")
        login = test_target_user.get("login")
        token = test_target_user.get("token")

        sql_string = (
            f"INSERT INTO target_users_vk (id_vk, login, token) VALUES ({id_vk}, %s, %s) "
            f"ON CONFLICT (id_vk) DO UPDATE SET "
            f"login = EXCLUDED.login , token = EXCLUDED.token "
            f"RETURNING *"
        )
        with self.connection_db:
            with self.connection_db.cursor() as cursor:
                cursor.execute(sql_string, [login, token])
        result = db_engine.get_token_vk_on_login(self.connection_db, login)
        self.assertEqual(result, token)

    def test_insert_update_user_vk(self):
        user_param = dict(test_target_user)
        user_id_vk = user_param.pop("id_vk")
        user_param.pop("id")
        user_param.pop("login")
        user_param.pop("token")
        result = db_engine.insert_update_user_vk(
            self.connection_db, "users_vk", user_id_vk, **user_param
        )
        self.assertEqual(
            [
                result[1],
                result[2],
                result[3],
                result[4],
                result[5],
                result[6],
                result[7],
                result[8],
                result[9],
                result[10],
                result[11],
                result[12],
                result[13],
                result[14],
            ],
            [
                test_target_user.get("id_vk"),
                test_target_user.get("first_name"),
                test_target_user.get("last_name"),
                test_target_user.get("sex"),
                test_target_user.get("city"),
                test_target_user.get("age"),
                test_target_user.get("activities"),
                test_target_user.get("interests"),
                test_target_user.get("movies"),
                test_target_user.get("music"),
                test_target_user.get("books"),
                test_target_user.get("quotes"),
                test_target_user.get("about"),
                test_target_user.get("home_town"),
            ],
        )

    def test_insert_update_grade_users(self):
        dict_test = dict(test_grade1)
        target_id = dict_test.pop("target_users_id")
        users_id = dict_test.pop("users_id")
        with self.connection_db as con:
            with con.cursor() as curs:
                curs.execute(
                    f"select * from grade_users where target_users_id = {target_id}"
                    f"and users_id = {users_id}"
                )
                curs.fetchall()

                target_param = dict(test_target_user)
                id_vk_target = target_param.pop("id_vk")
                db_engine.insert_update_user_vk(
                    con, "target_users_vk", id_vk_target, **target_param
                )

                user_param1 = dict(test_user1)
                id_vk1 = user_param1.pop("id_vk")
                db_engine.insert_update_user_vk(con, "users_vk", id_vk1, **user_param1)

        with self.connection_db as con:
            result = db_engine.insert_update_grade_users(
                con, target_id, users_id, **dict_test
            )

        self.assertEqual(
            result,
            (
                test_grade1.get("id"),
                test_grade1.get("target_users_id"),
                test_grade1.get("users_id"),
                test_grade1.get("points_auto"),
                test_grade1.get("points_user"),
                test_grade1.get("num_common_friends"),
                test_grade1.get("num_common_groups"),
                test_grade1.get("export_state"),
            ),
        )

    def test_get_grade_users(self):
        with self.connection_db as con:
            target_param = dict(test_target_user)
            id_vk_target = target_param.pop("id_vk")
            db_engine.insert_update_user_vk(
                con, "target_users_vk", id_vk_target, **target_param
            )

            user_param1 = dict(test_user1)
            id_vk1 = user_param1.pop("id_vk")
            db_engine.insert_update_user_vk(con, "users_vk", id_vk1, **user_param1)

            dict_test = dict(test_grade1)
            target_id = dict_test.pop("target_users_id")
            users_id = dict_test.pop("users_id")
            db_engine.insert_update_grade_users(con, target_id, users_id, **dict_test)

        result = db_engine.get_grade_users(con, target_id, id_vk1, False)
        self.assertEqual(
            result,
            (
                test_grade1.get("users_id"),
                test_grade1.get("num_common_groups"),
                test_grade1.get("points_auto"),
            ),
        )
        result = db_engine.get_grade_users(
            self.connection_db, target_id, users_id, True
        )
        self.assertIsNone(result)

    def test_top_from_grade_users(self):
        con = self.connection_db
        count_top = 1000
        with con:
            target_param = dict(test_target_user)
            id_vk_target = target_param.pop("id_vk")
            target_id = target_param.get("id")
            db_engine.insert_update_user_vk(
                con, "target_users_vk", id_vk_target, **target_param
            )

            user_param1 = dict(test_user1)
            id_vk1 = user_param1.pop("id_vk")
            user_param1.pop("id")

            dict_test = dict(test_grade1)
            dict_test.pop("target_users_id")
            dict_test.pop("users_id")
            dict_test.pop("id")
            points_auto = dict_test.pop("points_auto")

            for num in range(1, count_top + 1):
                user_param1.update({"id": num})
                dict_test.update(points_auto=points_auto + num)
                db_engine.insert_update_user_vk(
                    con, "users_vk", id_vk1 + num, **user_param1
                )
                db_engine.insert_update_grade_users(con, target_id, num, **dict_test)

        result = db_engine.top_from_grade_users(con, count_top, target_id)
        for num, grade in enumerate(result):
            self.assertEqual(grade[3], points_auto + count_top - num)

    def test_get_user_of_id_vk(self):
        con = self.connection_db
        with con:
            target_param = dict(test_target_user)
            id_vk_target = target_param.pop("id_vk")
            target_id = target_param.get("id")
            db_engine.insert_update_user_vk(
                con, "target_users_vk", id_vk_target, **target_param
            )

            user_param1 = dict(test_user1)
            id_vk1 = user_param1.pop("id_vk")
            user_id1 = user_param1.get("id")
            db_engine.insert_update_user_vk(con, "users_vk", id_vk1, **user_param1)

        result = db_engine.get_user_of_id_vk(con, "target_users_vk", id_vk_target)
        self.assertEqual(
            [result[0], result[1], result[8]],
            [target_id, id_vk_target, target_param.get("age")],
        )

        result = db_engine.get_user_of_id_vk(con, "users_vk", id_vk1)
        self.assertEqual(
            [result[0], result[1], result[6]],
            [user_id1, id_vk1, user_param1.get("age")],
        )

    def test_get_users_of_id_vk(self):
        con = self.connection_db
        count_users = 10
        user_param1 = dict(test_user1)
        id_vk1 = user_param1.pop("id_vk")
        user_param1.pop("id")
        with con:
            list_ids = []
            for num in range(1, count_users + 1):
                result = db_engine.insert_update_user_vk(
                    con, "users_vk", id_vk1 + num, **user_param1
                )
                list_ids.append(result[0])

        result = db_engine.get_users_of_id_vk(con, "users_vk", list_ids, True)
        self.assertEqual(len(result), len(list_ids))

    def test_count_in_table(self):
        con = self.connection_db
        count_users = 10
        target_param = dict(test_target_user)
        id_vk_target = target_param.pop("id_vk")
        target_param.pop("id")
        with con:
            for num in range(1, count_users + 1):
                db_engine.insert_update_user_vk(
                    con, "target_users_vk", id_vk_target + num, **target_param
                )
            db_engine.insert_update_user_vk(
                con, "target_users_vk", id_vk_target + 1, **target_param
            )
            db_engine.insert_update_user_vk(
                con, "target_users_vk", id_vk_target + 2, **target_param
            )

        result = db_engine.count_in_table(con, "target_users_vk")
        self.assertEqual(result, count_users)

    def test_all_items_in_table(self):
        con = self.connection_db
        count_users = 100
        user_param1 = dict(test_user1)
        id_vk1 = user_param1.pop("id_vk")
        user_param1.pop("id")
        with con:
            list_ids = []
            for num in range(1, count_users + 1):
                result = db_engine.insert_update_user_vk(
                    con, "users_vk", id_vk1 + num, **user_param1
                )
                list_ids.append(result[0])

        result = db_engine.all_items_in_table(con, "users_vk")
        self.assertIsInstance(result, list)
        self.assertEqual(result[2][1], id_vk1 + 3)
        self.assertEqual(len(result), count_users)

    def test_black_list(self):
        con = self.connection_db
        count_users = 10
        with con:
            target_param = dict(test_target_user)
            id_vk_target = target_param.pop("id_vk")
            target_id = target_param.get("id")
            db_engine.insert_update_user_vk(
                con, "target_users_vk", id_vk_target, **target_param
            )

            user_param1 = dict(test_user1)
            id_vk1 = user_param1.pop("id_vk")
            user_param1.pop("id")

            dict_test = dict(test_grade1)
            dict_test.pop("target_users_id")
            dict_test.pop("users_id")
            dict_test.pop("id")

            for num in range(1, count_users + 1):
                user_param1.update({"id": num})
                dict_test.update(points_user=(-1 if (num % 2) == 0 else num))
                db_engine.insert_update_user_vk(
                    con, "users_vk", id_vk1 + num, **user_param1
                )
                db_engine.insert_update_grade_users(con, target_id, num, **dict_test)

        with patch("db_engine.input", side_effect=["нет"]):
            result = db_engine.black_list(con, target_id)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), count_users // 2)

    def test_get_id_vk_vkapi_of_login(self):
        self.login = USER_FIX
        self.password = USER_PASS
        self.vk_v = VK_V
        self.application_id = APPLICATION_ID
        with patch("vkinder.getpass", return_value="USER_PASS"):
            result = vkinder.get_id_vk_vkapi_of_login(
                self.connection_db, self.login, self.vk_v, self.application_id
            )

        self.assertIsNone(result)

        with patch("vkinder.getpass", return_value=USER_PASS):
            result = vkinder.get_id_vk_vkapi_of_login(
                self.connection_db, self.login, self.vk_v, self.application_id
            )

        self.assertIsInstance(result[0], int)
        self.assertIsInstance(result[1], vk.API)


class TestApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # api
        cls.login = USER_FIX
        cls.password = USER_PASS
        cls.vk_v = VK_V
        cls.application_id = APPLICATION_ID
        session_vk_login = vk.AuthSession(cls.application_id, cls.login, cls.password,)
        session_vk = vk.Session(session_vk_login.access_token)
        cls.vk_api = vk.API(session_vk, timeout=60, v=cls.vk_v)

    def setUp(self) -> None:
        pass

    def test_vk_execute_for_method(self):
        q1 = "Москва"
        q2 = "Скворцово"
        list_param = [dict(q=q1, country_id=1), dict(q=q2, country_id=1)]
        result = vkapi.vk_execute_for_method(
            self.vk_api, "database.getCities", list_param
        )

        self.assertIsInstance(result, list)
        self.assertEqual(result[0].get("count"), len(result[0].get("items")))
        for city in result[0].get("items"):
            self.assertGreaterEqual(city.get("title").lower().find(q1.lower()), 0)
        self.assertEqual(result[1].get("count"), len(result[1].get("items")))
        for city in result[1].get("items"):
            self.assertGreaterEqual(city.get("title").lower().find(q2.lower()), 0)

    def test_find_group_all_users_list_25(self):
        pass

    def test_get_groups_of_ids(self):
        list_ids = list(range(1, 11))
        result = vkapi.get_groups_of_ids(list_ids, self.vk_api)
        self.assertIsInstance(result, list)
        for group in result:
            self.assertIn(group.get("id"), list_ids)
            self.assertIsInstance(group.get("screen_name"), str)

    def test_api_reuest(self):
        q1 = "Москва"
        params = dict(q=q1, country_id=1)
        result = vkapi.api_reuest(self.vk_api, "database.getCities", **params)
        self.assertIsInstance(result, dict)
        self.assertEqual(result.get("count"), len(result.get("items")))
        for city in result.get("items"):
            self.assertGreaterEqual(city.get("title").lower().find(q1.lower()), 0)

    def test_get_best_photo(self):
        user_vk_id = 1
        result = vkapi.get_best_photo(self.vk_api, user_vk_id)
        print(result)
        self.assertIsInstance(result, list)
        for image in result:
            self.assertRegex(image, r"^https://[^\s]+jpg")


if __name__ == "__main__":
    unittest.main()
