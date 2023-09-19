from bs4 import BeautifulSoup
import os
from os import walk



# собираем спаршенное в словарь
filenames = next(walk('parse_false'), (None, None, []))[2]
parsed_smi_webpages = [el[:-4] for el in filenames]




def transform_wrong_records_txt_to_dict(file) -> dict:
    """Считывает файл с неверными записями о СМИ и преобразовывает их в словарь"""
    false_url = {}  # собираем в словарь [номер неверной записи: url]

    with open(file, 'r', encoding="utf8") as f:
        wrong_records = f.read().splitlines()

    for record in wrong_records:
        number, url = record.split(' ')
        false_url[number] = url

    return false_url


def repare_number_and_url(false_dict):
    """Создаёт словарь с id и полным url, которых не хватает в спаршенных веб-страницах"""
    repared_info_dict = {}
    for number, full_url in false_dict.items():
        folder_name = full_url[27:]

        if folder_name in parsed_smi_webpages:
            repared_info_dict[number] = full_url
    return repared_info_dict


def reparse_raw_page(repared_number_url_dict):
    """Парсит инфо с сохраненных веб-страниц, переносит их в отдельные папки и возвращает словарь обработанного"""
    correct_record = {}

    for number, url in repared_number_url_dict.items():
        folder_name = url[27:]
        with open(f'parse_false\\{folder_name}.txt', 'r', encoding='utf8') as page:
            answer = page.read()
            soup = BeautifulSoup(answer, 'lxml')
            raw = soup.find('div', {"class": "news-smi-info mg-grid__item"})

            description = raw.find('div', {"class": "news-smi-info__description"}).text
            chief_name = raw.findAll('span', {"class": "news-smi-info__contacts-value"})[0].text
            address = raw.findAll('span', {"class": "news-smi-info__contacts-value"})[1].text
            phone = raw.findAll('span', {"class": "news-smi-info__contacts-value"})[2].text
            site = raw.find('a', {"class": "news-smi-info__contacts-value news-smi-info__contacts-value_link"}).get(
                'href')
            mail = raw.find('a', {"class": "news-smi-info__contacts-email"}).get('href')

            correct_record[number] = [url, description, chief_name, address, phone, site, mail]
            print(number, url, description, chief_name, address, phone, site, mail)
            os.mkdir(f'wrong_to_correct_bd\\{folder_name}')

            with open(f'wrong_to_correct_bd\\{folder_name}\\webpage_raw.txt', 'w', encoding='utf8') as f:
                f.write(answer)

    return correct_record


def wright_results_to_file(records_dict):
    """Дописывает словарь спаршенного в файлик corrected_wrong_records.txt"""
    with open('corrected_wrong_records.txt', 'a', encoding='utf8') as f:
        for k, v in records_dict.items():
            f.write(f'{k} {v}\n')


false_dict = transform_wrong_records_txt_to_dict('wrong_records.txt')  # получили словарь недопарсенных сайтов
repared_dict = repare_number_and_url(false_dict)
good_record = reparse_raw_page(repared_dict)
wright_results_to_file(good_record)
