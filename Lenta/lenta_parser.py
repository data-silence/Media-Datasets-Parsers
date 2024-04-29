import asyncio
import aiohttp
import asyncpg

import datetime as dt
from bs4 import BeautifulSoup
import random
from loguru import logger
import re
import pickle
import pandas as pd

DB_NAME = 'postgres'
DB_PASS = "password"
DB_USER = "postgres"
DB_HOST = "localhost"
DB_PORT = 5432

con = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
q = f"select url from news where date is null"

links = pd.read_sql(f"select url from news where date is null", con).url.tolist()


# Open the pickle file for reading in binary mode
# with open('links.pkl', 'rb') as file:
#     links = pickle.load(file)


async def write_to_db(url, date, links):
    """Writes the news to the database"""
    conn = await asyncpg.connect(con)
    await conn.fetch('UPDATE news SET date = $1, links=$2 WHERE url = $3', date, links, url)
    await conn.close()
    # await asyncio.sleep(0.1)


async def fetch_content(url, session):
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}

    async with session.get(url=url, headers=headers) as response:
        if response and response.status != 204:
            answer = await response.text()
            try:
                soup = BeautifulSoup(answer, features="html.parser")
                raw_time = soup.body.find(attrs={'class': 'topic-header__item topic-header__time'}).text
                regex = re.compile(r'\d{4}/\d{2}/\d{2}')
                raw_date = regex.findall(url)[0]
                news_date = dt.datetime.fromisoformat(raw_date.replace('/', '-') + ' ' + raw_time.split(',')[0])
                try:
                    links = soup.body.find(attrs={'class': 'related-topics__list'}).find_all(
                        attrs={'class': 'related-topics__link'})
                    links = ['https://lenta.ru/' + link.get('href') for link in links]
                except Exception:
                    links = []
                await write_to_db(url=url, date=news_date, links=links)
            except AttributeError:
                logger.info(f'{url} не был записан')


async def main():
    chunk = 100
    tasks = []
    start = 0

    times = len(links) // chunk + 1

    for el in range(times):
        logger.info(f'Обработка {el + 1}/{times}')
        try:
            async with aiohttp.ClientSession() as session:
                for url in links[start:start + chunk]:
                    task = asyncio.create_task(fetch_content(url, session))
                    tasks.append(task)

                await asyncio.gather(*tasks)
                await asyncio.sleep(0.2)
            start += chunk
            tasks = []
        except Exception:
            logger.error(f'Ошибка обработки {el}/{times}')


if __name__ == '__main__':
    asyncio.run(main())
