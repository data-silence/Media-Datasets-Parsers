import asyncio
import aiohttp
import asyncpg

from bs4 import BeautifulSoup
import random
from loguru import logger
import dateparser
import datetime as dt
import time
import pickle
import pandas as pd
from typing import List, Optional

# User agents for HTTP requests
user_agents = open('proxy/user-agents.txt').read().splitlines()
random_user_agent = random.choice(user_agents)
headers = {'User-Agent': random_user_agent}

# Database connection parameters
DB_USER = 'DB_USER'
DB_NAME = 'DB_NAME'
DB_PASS = "DB_PASS"
DB_HOST = "DB_HOST"
DB_PORT = 5432

con = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# q = f"SELECT url FROM lenta where news is null"
# links = pd.read_sql(sql=q, con=con).url.tolist()

# List to store links for news articles
links: List[str] = []
next_links: List[str] = []


def save_list_to_file(my_list: List[str], filename: str) -> None:
    """
    Saves a list to a file using pickle.

    :param my_list: The list to save
    :param filename: The filename to save the list to
    """
    with open(filename, 'wb') as file:
        pickle.dump(my_list, file)
        logger.info(f"List saved successfully to {filename}.")


def load_list_from_file(filename: str) -> List[str]:
    """
    Loads a list from a file using pickle.

    :param filename: The filename to load the list from
    :return: The loaded list
    """
    with open(filename, 'rb') as file:
        loaded_list = pickle.load(file)
        logger.info(f"List loaded successfully from {filename}.")
        return loaded_list


async def write_to_db(url: str, date: Optional[dt.datetime], title: Optional[str], topic: Optional[str],
                      news: Optional[str], tags: Optional[str]) -> None:
    """
    Writes the news data to the database.

    :param url: The URL of the news article
    :param date: The publication date of the news article
    :param title: The title of the news article
    :param topic: The topic of the news article
    :param news: The content of the news article
    :param tags: The tags associated with the news article
    """
    conn = await asyncpg.connect(con)
    query = """
    INSERT INTO lenta (url, date, title, topic, news, tags, links, resume) 
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
    await conn.execute(query, *values)
    logger.info(f"{url} записан успешно")
    await conn.close()


async def fetch_page_urls(url: str, session: aiohttp.ClientSession) -> None:
    """
    Fetches URLs of news articles from a given page.

    :param url: The URL of the page to fetch URLs from
    :param session: The aiohttp session for making HTTP requests
    """
    async with session.get(url=url, headers=headers) as response:
        try:
            if response and response.status != 204:
                answer = await response.text()
                soup = BeautifulSoup(answer, features="html.parser")
                paragraph = soup.body.find_all(attrs={'class': 'card-full-news _archive'})
                date_list = ['https://lenta.ru' + el.get('href') for el in paragraph]
                links.extend(date_list)
                load_mores = soup.find_all("a", class_="loadmore js-loadmore _two-buttons")
                for load_more in load_mores:
                    if load_more and "Дальше" in load_more.get_text() and "_disabled" not in load_more.get("class", []):
                        next_page_url = f"https://lenta.ru{load_more['href']}"
                        next_links.append(next_page_url)
                        break
        except AttributeError:
            logger.error(url)


async def fetch_content(url: str, session: aiohttp.ClientSession) -> None:
    """
    Fetches the content of a news article from a given URL.

    :param url: The URL of the news article
    :param session: The aiohttp session for making HTTP requests
    """
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


def get_all_links(start_parse_date: dt.date, end_parse_date: dt.date) -> None:
    """
    Fetches all links of news articles within a date range.

    :param start_parse_date: The start date for fetching links
    :param end_parse_date: The end date for fetching links
    """
    global next_links
    dates_list = pd.date_range(start=start_parse_date, end=end_parse_date, freq="D").date.tolist()
    top_urls_list = ['https://lenta.ru/news/' + el.strftime('%Y/%m/%d') for el in dates_list]

    asyncio.run(get_async_job(target_urls=top_urls_list, async_func=fetch_page_urls))
    while next_links:
        top_urls_list = next_links.copy()
        next_links = []
        asyncio.run(get_async_job(target_urls=top_urls_list, async_func=fetch_page_urls))
        time.sleep(0.1)

    logger.info(f'Всего собрано {len(links)} ссылок')
    save_list_to_file(my_list=links, filename='links_2024.pkl')


async def get_async_job(target_urls: List[str], async_func) -> None:
    """
    Runs asynchronous jobs to fetch data from URLs.

    :param target_urls: The list of URLs to fetch data from
    :param async_func: The asynchronous function to run for each URL
    """
    chunk = 90  # Reduce this if the database complains about too many connections
    tasks = []
    start = 0

    logger.info(f'Начинается сбор новостей')
    times = len(target_urls) // chunk + 1
    for el in range(times):
        logger.info(f'Обработка {el + 1}/{times}')
        async with aiohttp.ClientSession() as session:
            for url in target_urls[start:start + chunk]:
                task = asyncio.create_task(async_func(url, session))
                tasks.append(task)

            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)
        start += chunk
        tasks = []


if __name__ == '__main__':
    get_all_links(start_parse_date=dt.date(2024, 6, 17), end_parse_date=dt.date(2024, 6, 18))
    if not links:
        links = load_list_from_file('links_2024.pkl')
    asyncio.run(get_async_job(target_urls=links, async_func=fetch_content))
