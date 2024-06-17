import asyncio
import aiohttp
import asyncpg

from bs4 import BeautifulSoup
import requests
import random
from loguru import logger
import dateparser
import datetime as dt
import time
import pickle

# import pickle
# import pandas as pd
# import nest_asyncio
# nest_asyncio.apply()




con = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

q = f"SELECT url FROM new_lenta where news is null"


# links = pd.read_sql(sql=q, con=con).url.tolist()

def get_one_page_urls(agency_url: str) -> tuple:
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}
    answer = requests.get(agency_url, headers=headers)
    next_page_url = None
    date_list = []
    try:
        if answer and answer.status_code != 204:
            soup = BeautifulSoup(answer.text, features="html.parser")
            paragraph = soup.body.find_all(attrs={'class': 'card-full-news _archive'})
            date_list = [{'url': 'https://lenta.ru' + el.get('href'),
                          'date': dateparser.parse(el.time.text), 'news': el.h3.text, 'tags': el.span.text} for el in
                         paragraph]
            load_mores = soup.find_all("a", class_="loadmore js-loadmore _two-buttons")
            for load_more in load_mores:
                if load_more and "Дальше" in load_more.get_text() and "_disabled" not in load_more.get("class", []):
                    next_page_url = f"https://lenta.ru{load_more['href']}"
                    break
    except AttributeError:
        logger.error(agency_url)
    return date_list, next_page_url


def get_all_urls(current_date: dt.date, end_date: dt.date) -> list:
    links = []
    while current_date <= end_date:
        logger.info(f'Обработка {str(current_date)}')
        current_url = 'https://lenta.ru/news/' + current_date.strftime('%Y/%m/%d')
        date_list, next_page_url = get_one_page_urls(agency_url=current_url)
        if date_list:
            links.extend(date_list)
        while next_page_url:
            date_list, next_page_url = get_one_page_urls(agency_url=next_page_url)
            if date_list:
                links.extend(date_list)
            time.sleep(0.1)
        current_date += dt.timedelta(days=1)
        time.sleep(0.1)
    return links


def save_list_to_file(my_list, filename):
    """
    Сохраняет список в файл с использованием pickle.

    :param my_list: Список для сохранения
    :param filename: Имя файла для сохранения
    """
    with open(filename, 'wb') as file:
        pickle.dump(my_list, file)
        # print(f"List saved successfully to {filename}.")


def load_list_from_file(filename):
    """
    Загружает список из файла с использованием pickle.

    :param filename: Имя файла для загрузки
    :return: Загруженный список
    """
    with open(filename, 'rb') as file:
        loaded_list = pickle.load(file)
        # print(f"List loaded successfully from {filename}.")
        return loaded_list


async def write_to_db(url, date, title, topic, news, tags):
    """Writes the news to the database"""
    conn = await asyncpg.connect(con)
    query = """
    INSERT INTO new_lenta (url, date, title, topic, news, tags, links, resume) 
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    """
    values = (
        url,
        date,
        title,
        topic,
        news,
        tags,
        None,
        None
    )
    await conn.execute(query, values)
    logger.info(f"{url} записан успешно")
    await conn.close()


async def fetch_content(url, session) -> None:
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}

    async with session.get(url=url, headers=headers) as response:
        if response and response.status != 204:
            answer = await response.text()
            soup = BeautifulSoup(answer, features="html.parser")
            if date_soup := soup.find(attrs={'class': 'topic-header__item topic-header__time'}):
                date = dateparser.parse(date_soup.text)
            else:
                date = None
            title = soup.find(attrs={'class': 'topic-body__title'})
            topic = soup.find(attrs={'class': 'topic-header__item topic-header__rubric'})
            news = soup.find(attrs={'class': 'topic-body__content'})
            tags = soup.find(attrs={'class': 'rubric-header__link _active'})
            try:
                title, topic, news, tags = (el.text if el else None for el in (title, topic, news, tags))
            except AttributeError:
                logger.error(f'{url} косячный')
            await write_to_db(url=url, date=date, title=title, topic=topic, news=news, tags=tags)
            # await asyncio.sleep(0.2)


async def main() -> None:
    chunk = 90
    tasks = []
    start = 0

    start_parse_date = dt.date(2024, 1, 1)
    end_parse_date = dt.date(2024, 6, 16)

    logger.info(f'Начинается сбор ссылок за период с {start_parse_date} по {end_parse_date}')
    links = get_all_urls(current_date=start_parse_date, end_date=end_parse_date)
    logger.info(f'Всего собрано {len(links)} ссылок')
    save_list_to_file(my_list=links, filename='links_2024.pkl')
    logger.info('Список сохранён на диск')

    logger.info(f'Начинается сбор новостей')
    times = len(links) // chunk + 1
    for el in range(times):
        logger.info(f'Обработка {el + 1}/{times}')
        async with aiohttp.ClientSession() as session:
            for url in links[start:start + chunk]:
                task = asyncio.create_task(fetch_content(url, session))
                tasks.append(task)

            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)
        start += chunk
        tasks = []


if __name__ == '__main__':
    asyncio.run(main())
