# собираем библиотеку "удобняков"

import datetime
import re


# Счетчик времени для оценки затрат времени на операции
class date_logger:
    def __init__(self):
        self.date_start = datetime.datetime.now()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.date_stop = datetime.datetime.now()
        print(f"Время старта {self.date_start}")
        print(f"Время окочания {self.date_stop}")
        print(
            f"Затрачено для вычислений {(self.date_stop - self.date_start).seconds: .0F} секунд"
        )


# статус бар
class StatusBar:
    def __init__(self, amount_count, start_count=0):
        if isinstance(amount_count, int) and isinstance(start_count, int):
            self.amount_count = amount_count
            self.current_count = start_count

    def plus(self, plus_count=1):
        self.current_count += plus_count
        current_stat = int(100 / self.amount_count * self.current_count)
        status_bar = ("█" * current_stat) + str(current_stat) + "%"
        print("\r" + status_bar, end="" if current_stat < 100 else "\n")


# Фильтр только целые положительные числа
def posintput(string, start_num: int = 0, end_num: int = None):
    def warning():
        warning_str = f"Нужно ввести целое положительное число от {start_num}"
        if end_num:
            warning_str += f" до {end_num}"
        print(warning_str)

    while True:
        integer = input(string)
        if not integer.isdigit():
            warning()
        else:
            integer = int(integer)
            if integer < start_num:
                warning()
            elif end_num:
                if integer > end_num:
                    warning()
                else:
                    return integer


# list список слов из строки
def list_from_string(in_string):
    if isinstance(in_string, str):
        reg_interest = re.compile(r"[\W\( ]*\b([\d`а-яА-ЯёЁA-Za-z. -]{3,})\b[\W,\) ]*")
        result_list = re.findall(reg_interest, in_string, )
        result_list = list(map(lambda string: string.lower(), result_list))
        if result_list:
            return result_list
