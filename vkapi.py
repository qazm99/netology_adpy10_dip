# vk_api
import time
import vk

from qazm import StatusBar


# vk_api max len(params_list) == 25
def vk_execute_for_method(vk_api, method_vk, params_list):
    string_api_execute = "return ["
    for current_params in params_list:
        string_api_execute += f"API.{method_vk}(" + "{"
        for key, value in current_params.items():
            if isinstance(value, str):
                value = f"'{value}'"
            string_api_execute += f'"{key}":{value}, '
        string_api_execute = string_api_execute[: len(string_api_execute) - 2] + "}), "
    string_api_execute = string_api_execute[: len(string_api_execute) - 2] + "];"
    string_api_execute = string_api_execute.replace("\\", "")
    code_dict = {"code": string_api_execute}
    result = api_reuest(vk_api, "execute", **code_dict)
    return result


# vk_api
# быстрый поиск групп, в каждный запрос отсылаем по 25 пользователей
# на входе список пользователей на выходе список словарей {пользователь: список групп пользователя}
def find_group_all_users_list_25(all_users_ids_list, vk_api):
    users_all_group_list = []
    all_users_ids_25 = [
        all_users_ids_list[d: d + 25] for d in range(0, len(all_users_ids_list), 25)
    ]
    status_bar_find_group = StatusBar(len(all_users_ids_25))
    print("Сейчас мы поищем общие группы, чтобы лучше узнать об общих интересах")
    for num_user, users_ids_25 in enumerate(all_users_ids_25):
        status_bar_find_group.plus()
        string_api_execute = "return ["
        for user_id_25 in users_ids_25:
            string_api_execute += (
                'API.groups.get({"user_id":' + str(user_id_25) + "}), "
            )
        string_api_execute = string_api_execute[: len(string_api_execute) - 2] + "];"
        code_dict = {"code": string_api_execute}

        result_current_25 = api_reuest(vk_api, "execute", **code_dict)

        if result_current_25:
            for num_user_in_25, user_friend_group in enumerate(result_current_25):
                if isinstance(user_friend_group, dict):
                    users_all_group_list.append(
                        {users_ids_25[num_user_in_25]: user_friend_group["items"]}
                    )
                else:
                    users_all_group_list.append({users_ids_25[num_user_in_25]: []})
    return users_all_group_list


# vk_api поиск групп пользователя
def get_groups_of_ids(ids_list, vk_api):
    ids_string = ",".join(list(map(lambda id_str: str(id_str), ids_list)))
    groups = list(
        map(
            lambda group: {
                "id": group.get("id"),
                "screen_name": group.get("screen_name"),
            },
            vk_api.groups.getById(group_ids=ids_string),
        )
    )
    return groups


# vk_api
def api_reuest(vk_api, method, **kwargs):
    for num in range(1, 10):
        try:
            result = vk_api.__call__(method, **kwargs)
            if result:
                return result
        except vk.api.VkAPIError as e:
            if e.__str__()[:1] in ("6", "10"):
                time.sleep(2)
            elif e.__str__()[:2] == "15":
                print("Аккаунт заблокирован")
                break
            elif e.__str__()[:2] == "30":
                print("Приватный аккаунт")
                break
            else:
                print(e)


# vk_api
def get_best_photo(vk_api, user_vk_id):
    dict_photo_params = dict(owner_id=user_vk_id, album_id="profile", extended=1)
    dict_vk_photo = api_reuest(vk_api, "photos.get", **dict_photo_params)
    if isinstance(dict_vk_photo, dict):
        top_all = []
        for photo in dict_vk_photo.get("items"):
            top_all.append(
                (photo.get("likes").get("count"), photo.get("sizes")[-1].get("url"))
            )
        top_all.sort()
        top_3_photo = list(reversed(top_all[-3:]))
        top_3_photo = list(map(lambda photo_top: photo_top[1], top_3_photo))
        return top_3_photo
