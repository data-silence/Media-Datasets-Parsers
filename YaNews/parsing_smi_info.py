import requests
from bs4 import BeautifulSoup
import os
import json

log = []

def wright_to_json(filename, sth_name):
    with open(f'{filename}.json', 'w', encoding='utf8') as f:
        json.dump(sth_name, f)


def make_smi_profile(smi_link, folder_name):
    headers = {
        "user-agent": "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; Acoo Browser; GTB5; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; InfoPath.1; .NET CLR 3.5.30729; .NET CLR 3.0.30618)"
    }

    smi_db_json = {}
    answer = requests.get(smi_link, headers=headers)
    soup = BeautifulSoup(answer.text, 'lxml')

    raw = soup.find('dl', {"class": "smi__info"})
    try:
        description = raw.find('dd', {"class": "smi__description"}).text
    except AttributeError:
        description = None
        log.append(folder_name)
    try:
        chief_position = raw.find('dt', {"class": "smi__chief-title"}).text
    except AttributeError:
        chief_position = None
    try:
        chief_name = raw.find('dd', {"class": "smi__chief-value"}).text
    except AttributeError:
        chief_name = None
    try:
        address = raw.find('dd', {"class": "smi__address-value"}).text
    except AttributeError:
        address = None
    try:
        phone = raw.find('dd', {"class": "smi__address-value"}).text
    except AttributeError:
        phone = None
    try:
        site = raw.find('dd', {"class": "smi__website-value"}).text
    except AttributeError:
        site = None
    try:
        mail = raw.find('a', {"class": "link link_theme_normal smi__mailto i-bem"}).get('href')
    except AttributeError:
        mail = None
    try:
        logo_url = raw.find('img', {"class": "image"}).get('src')
        logo = requests.get(f"https:{logo_url}", headers=headers)
        with open(f'smi_bd\\{folder_name}\\logo.webp', 'wb') as f:
            f.write(logo.content)
    except:
        # print(f'Логотип для {smi_link} не сохранен')
        pass

    smi_db_json['url'] = smi_link
    smi_db_json['description'] = description
    smi_db_json['chief_position'] = chief_position
    smi_db_json['chief_name'] = chief_name
    smi_db_json['address'] = address
    smi_db_json['phone'] = phone
    smi_db_json['site'] = site
    smi_db_json['mail'] = mail

    with open(f'smi_db.json', 'a', encoding='utf8') as f:
        json.dump(smi_db_json, f)

    # try:
    os.mkdir(f'smi_bd\\{folder_name}')
    # except FileExistsError:
    #     pass
    # os.mkdir(f'articles\\{folder_name}')
    with open(f'smi_bd\\{folder_name}\\smi_profile.txt', 'w', encoding='utf8') as f:  # {smi_link[27:]}
        f.write(f'{description}\n')
        f.write(f'{chief_position} {chief_name}\n')
        f.write(f'Адрес: {address}\n')
        f.write(f'Телефон: {phone}\n')
        f.write(f'Сайт: {site}\n')
        f.write(f'{mail}\n')

with open('log_wrong_parsing.txt', 'a', encoding='utf8') as f:
    for el in log:
        f.write(f'{el}\n')

# print(raw.prettify())
# print(description)
# print(chief_position, chief_name)
# print(f'Адрес: {address}')
# print(f'Телефон: {phone}')
# print(f'Сайт: {site}')
# print(mail)
