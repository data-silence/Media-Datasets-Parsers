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


async def fetch_content(url):
    date_pattern = r'/(\d{4})/(\d{2})/(\d{2})(\d{2})(\d{2})'

    # Поиск даты в URL
    match = re.search(date_pattern, url)
    if match:
        year, month, day = match.group(1), match.group(2), match.group(5)
        extracted_date = f"{year}/{month}/{day}"
        news_date = dateparser.parse(extracted_date)
        # print(news_date, links)
        await write_to_db(url=url, date=news_date)
        logger.info(f'{url} записан успешно')
    else:
        pass


async def main():
    chunk = 50
    tasks = []
    start = 0

    times = len(links) // chunk + 1

    for el in range(times):
        logger.info(f'Обработка {el + 1}/{times}')
        for url in links[start:start + chunk]:
            task = asyncio.create_task(fetch_content(url))
            tasks.append(task)

            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)
        start += chunk
        tasks = []


if __name__ == '__main__':
    asyncio.run(main())
