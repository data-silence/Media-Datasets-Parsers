import asyncio
import aiohttp
import asyncpg

from bs4 import BeautifulSoup
import random
from loguru import logger
import pandas as pd
import dateparser

DB_NAME = 'name'
DB_PASS = "pass"
DB_USER = "user"
DB_HOST = "host"
DB_PORT = 5432

con = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
q = f"select url from news where agency = 'Lenta' and date::time = '00:00:00'"

links = pd.read_sql(sql=q, con=con).url.tolist()


async def write_to_db(url, date):
    """Writes the news to the database"""
    # logger.info(f"Writing {date} to database url {url}")
    conn = await asyncpg.connect(con)
    await conn.fetch('UPDATE news SET date = $2 WHERE url=$1', url, date)
    await conn.close()


async def fetch_content(url, session):
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}

    async with session.get(url=url, headers=headers) as response:
        if response and response.status != 204:
            answer = await response.text()
            soup = BeautifulSoup(answer, features="html.parser")
            try:
                raw_time = soup.body.find(attrs={'class': 'topic-header__item topic-header__time'}).text
                news_date = dateparser.parse(raw_time)
                await write_to_db(url=url, date=news_date)
                logger.info(f'{url} записан успешно')
            except AttributeError as e:
                logger.info(f'{url} не был записан')
                # print(e)


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
        # except Exception as e:
        #     logger.error(f'Ошибка обработки {el}/{times}')


if __name__ == '__main__':
    asyncio.run(main())
