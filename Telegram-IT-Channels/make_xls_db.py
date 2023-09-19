import glob
from openpyxl import Workbook
import shlex

files_list = glob.glob("smi_bd\\*")
headers = dict(A='id', B='url', C='ya_link', D='description', E='email', F='phone', G='address', H='chief')

wrong_records = {}
workbook = Workbook()
extend_dict = workbook.active  # активировали xls-лист, в котором будем записывать БД.


def make_db_headers(dict_headers):
    """Создаёт на рабочем листе строку заголовков"""
    for letter in dict_headers:
        db_full[letter + '1'] = dict_headers[letter]


def fill_db(filenames_list):
    """Заполняет базу данных из спарсенных txt-файлов в соответствии с разметкой заголовков + собирает список косяков"""
    number = 0

    for files in filenames_list:
        with open(f'{files}', 'r', encoding="utf8", errors='ignore') as f:
            smi_data = f.read().splitlines()
            folder_name = files.replace('smi_bd', '').replace('.txt', '').strip()[1:]
            try:
                # print(f'записываю {files}')
                db_full['A' + str(number + 2)] = (number + 1)  # smi_id
                db_full['B' + str(number + 2)] = smi_data[4].split(sep=': ')[1]  # url
                db_full['C' + str(number + 2)] = 'https://news.yandex.ru/smi/' + folder_name  # ya_link
                db_full['D' + str(number + 2)] = smi_data[0]  # description
                db_full['E' + str(number + 2)] = smi_data[5].split(sep=':')[1]  # email
                db_full['F' + str(number + 2)] = smi_data[3].split(sep=': ')[1]  # phone
                db_full['G' + str(number + 2)] = smi_data[2].split(sep=': ')[1]  # address
                db_full['H' + str(number + 2)] = smi_data[1].split(sep=': ')[1]  # chief_name
            except IndexError:
                wrong_records[number + 1] = 'https://news.yandex.ru/smi/' + folder_name  # записали косяк в словарь

            number += 1
    return wrong_records


def fill_wrong_records_log(wrongs):
    with open('wrong_records.txt', 'a', encoding='utf8') as f:
        for k, v in wrongs.items():
            f.write(f'{k} {v}\n')

def fill_corrected_bd(files):
    # extend_dict = {}
    with open(files, mode='r', encoding='utf8') as file:
        string = file.readlines()
        for j in range(len(string)):
            number = int(string[j].split(maxsplit=1)[0])
            extend_dict['A' + str(number + 1)] = number  # smi_id
            smi_info = (string[j].split(maxsplit=1)[1])[1:-2]
            smi_info = shlex.split(smi_info)
            extend_dict['B' + str(number + 1)] = smi_info[5][:-1]  # url
            extend_dict['C' + str(number + 1)] = smi_info[0][:-1]  # ya_link
            extend_dict['H' + str(number + 1)] = smi_info[1][:-1]  # description
            extend_dict['D' + str(number + 1)] = smi_info[6][7:]  # email
            extend_dict['E' + str(number + 1)] = smi_info[4][:-1]  # phone
            extend_dict['F' + str(number + 1)] = smi_info[3][:-1]  # address
            extend_dict['G' + str(number + 1)] = smi_info[2][:-1]  # chief_name
    return extend_dict



# make_db_headers(headers)  # заполнили строку заголовков
# fill_db(files_list)  # заполнили табличные данные в соответствие с шаблоном заголовков
# fill_wrong_records_log(wrong_records)  # сохранили косячные данные в отдельный текстовой файл
# workbook.save(filename="test.xlsx")  # сохранили результаты работы
my_dict = fill_corrected_bd('corrected_wrong_records.txt')

workbook.save(filename="test.xlsx")  # сохранили результаты работы