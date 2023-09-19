import random
import requests
from bs4 import BeautifulSoup
import os
import time
import socks
import socket

user_agents = open('proxy/user-agents.txt').read().splitlines()
random_user_agent = random.choice(user_agents)

headers = {'User-Agent': random_user_agent}

socks.set_default_proxy(socks.SOCKS5, "localhost", 9150)
socket.socket = socks.socksocket


def transform_wrong_records_txt_to_dict(file) -> dict:
    """Считывает файл с неверными записями о СМИ и преобразовывает их в словарь"""
    false_url = {}  # собираем в словарь [номер неверной записи: url]

    with open(file, 'r', encoding="utf8") as f:
        wrong_records = f.read().splitlines()

    for record in wrong_records:
        number, url = record.split(' ')
        false_url[number] = url

    return false_url


def reparse_wrong_records(false_record_dict: dict) -> dict:
    correct_record = {}
    # fail_record = {}
    delay_time = random.randint(3, 5)

    for number, url in false_record_dict.items():
        answer = requests.get(url, headers=headers, timeout=10).text
        folder_name = url.split("/")[-1]
        soup = BeautifulSoup(answer, 'lxml')
        raw = soup.find('div', {"class": "news-smi-info mg-grid__item"})

        print(f'Записываю {number} {url}')

        for i in range(5):
            try:
                description = raw.find('div', {"class": "news-smi-info__description"}).text
                chief_name = raw.findAll('span', {"class": "news-smi-info__contacts-value"})[0].text
                address = raw.findAll('span', {"class": "news-smi-info__contacts-value"})[1].text
                phone = raw.findAll('span', {"class": "news-smi-info__contacts-value"})[2].text
                site = raw.find('a', {"class": "news-smi-info__contacts-value news-smi-info__contacts-value_link"}).get(
                    'href')
                mail = raw.find('a', {"class": "news-smi-info__contacts-email"}).get('href')

                correct_record[number] = [url, description, chief_name, address, phone, site, mail]

                os.mkdir(f'wrong_to_correct_bd\\{folder_name}')

                with open(f'wrong_to_correct_bd\\{folder_name}\\webpage_raw.txt', 'w', encoding='utf8') as f:
                    f.write(answer)

                print(f'Успешно {headers}')
                time.sleep(delay_time)
                break

            except:
                continue
                # fail_record[number] = url
                # print(f'...что-то пошло не так...')

    return correct_record


def wright_results_to_file(records_dict):
    with open('corrected_wrong_records.txt', 'a', encoding='utf8') as f:
        for k, v in records_dict.items():
            f.write(f'{k} {v}\n')


false = transform_wrong_records_txt_to_dict('wrong_records.txt')
correct = reparse_wrong_records(false)
wright_results_to_file(correct)


