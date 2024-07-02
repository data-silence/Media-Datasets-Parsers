import asyncio
import aiohttp
import asyncpg

import datetime as dt
from bs4 import BeautifulSoup
import random
from loguru import logger
import re
import pandas as pd
import dateparser

# Database connection parameters
DB_USER = 'DB_USER'
DB_NAME = 'DB_NAME'
DB_PASS = "DB_PASS"
DB_HOST = "DB_HOST"
DB_PORT = 5432

con = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
q = f"select url from bbc where date is null"

links = pd.read_sql(sql=q, con=con).url.tolist()


async def write_to_db(url, date):
    """Writes the news to the database"""
    conn = await asyncpg.connect(con)
    await conn.fetch('UPDATE bbc SET date = $2 WHERE url=$1', url, date)
    await conn.close()
    # await asyncio.sleep(0.1)


async def fetch_content(url, session):
    user_agents = open('../Newsruilcom/proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}

    async with session.get(url=url, headers=headers) as response:
        if response and response.status != 204:
            answer = await response.text()
            soup = BeautifulSoup(answer, features="html.parser")
            try:
                raw_time = soup.body.find(attrs={'class': 'datestamp'}).text
                raw_date = raw_time.split('\xa0')[1].strip()
                date_string = (raw_date.split(', ')[1] + ' ' + raw_date.split(', ')[2].split('GMT ')[-1]).replace(
                    ' MCK', '')
                news_date = dateparser.parse(date_string)
                # print(news_date, links)
                await write_to_db(url=url, date=news_date)
                logger.info(f'{url} записан успешно')
            except AttributeError:
                pass



async def main():
    chunk = 50
    tasks = []
    start = 0

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
