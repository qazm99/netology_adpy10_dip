# test_calc_common_param
list_first_for_calc = ["one", "two", 3, {4: 5}]
list_second_for_calc = ["two", {4: 5}]
list_wrong_for_calc = [6, "seven", {8: "nine"}]

struct_table_test = dict(
    table_name="test_table_vk",
    parameters=(
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
)

test_target_user = {
    "id": 1,
    "id_vk": 999999,
    "login": "test login",
    "token": "026f8e459c8f89ef75fa7a78265a0025",
    "first_name": "target first name",
    "last_name": "target last name",
    "sex": 2,
    "city": 1,
    "age": 99,
    "activities": ["activities1", "activities2"],
    "interests": ["interests1", "interests2"],
    "movies": ["movies1", "movies2"],
    "music": ["music1", "music2"],
    "books": ["books1", "books2"],
    "quotes": ["quotes1", "quotes2"],
    "about": ["about1", "about2"],
    "home_town": ["home_town1", "home_town2"],
}

test_user1 = {
    "id": 2,
    "id_vk": 777777,
    "first_name": "user first name",
    "last_name": "user last name",
    "sex": 1,
    "city": 1,
    "age": 33,
    "activities": ["activities1", "activities2"],
    "interests": ["interests1", "interests2"],
    "movies": ["movies1", "movies2"],
    "music": ["music1", "music2"],
    "books": ["books1", "books2"],
    "quotes": ["quotes1", "quotes2"],
    "about": ["about1", "about2"],
    "home_town": ["home_town1", "home_town2"],
    "relation": 1,
}

test_user2 = {
    "id": 3,
    "id_vk": 888888,
    "first_name": "user first name",
    "last_name": "user last name",
    "sex": 1,
    "city": 1,
    "age": 55,
    "activities": ["activities1", "activities2"],
    "interests": ["interests1", "interests2"],
    "movies": ["movies1", "movies2"],
    "music": ["music1", "music2"],
    "books": ["books1", "books2"],
    "quotes": ["quotes1", "quotes2"],
    "about": ["about1", "about2"],
    "home_town": ["home_town1", "home_town2"],
    "relation": 6,
}

test_grade1 = {
    "id": 1,
    "target_users_id": 1,
    "users_id": 2,
    "points_auto": 100,
    "points_user": 90,
    "num_common_friends": 10,
    "num_common_groups": 20,
    "export_state": True,
}

test_grade2 = {
    "id": 2,
    "target_users_id": 1,
    "users_id": 3,
    "points_auto": 50,
    "points_user": 70,
    "num_common_friends": 5,
    "num_common_groups": 30,
    "export_state": False,
}
