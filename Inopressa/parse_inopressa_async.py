import asyncio
import aiohttp
import pickle
from imports_shop import *
import random
from loguru import logger
from time import sleep
import asyncpg
from db import *


time_machine = create_engine(
    f'postgresql+psycopg2://{DB_NAME}:{DB_PASS}@{DB_HOST}/{DB_NAME}', pool_pre_ping=True)


# with open('temp/new_list.pickle', 'rb') as f:
#     inopressa_list = pickle.load(f)

def get_list():
    q = f"select url from news where news.news = 'NaN' and news.url like 'https://www.inopressa.ru/%'"
    result = DataBaseMixin.get(q, time_machine)
    inopress_list = [el['url'] for el in result]
    return inopress_list





async def write_to_db(url, news):
    conn = await asyncpg.connect(f'postgresql://{DB_NAME}:{DB_PASS}@{DB_HOST}/{DB_NAME}')
    await conn.fetch('UPDATE news SET news=$1 WHERE url=$2', news, url)
    await conn.close()
    await asyncio.sleep(0.1)


async def fetch_content(url, session):
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}

    async with session.get(url=url, headers=headers) as response:

        answer = await response.text()
        try:
            soup = BeautifulSoup(answer, features="html.parser")
            news = soup.body.table.find_all('tr')[1].td.find_all('table')[5].find_all(attrs={'class': 'body'})[
                0].text
            # print(news)
            await write_to_db(url=url, news=news)
            logger.info(f'{url} записан успешно')
        except Exception:
            logger.info(f'{url} не был записан')


async def main():
    chunk = 168
    tasks = []
    start = 0
    inopressa_list = get_list()

    for el in range(len(inopressa_list) // chunk):
        async with aiohttp.ClientSession() as session:
            for url in inopressa_list[start:start + chunk]:
                task = asyncio.create_task(fetch_content(url, session))
                tasks.append(task)

            await asyncio.gather(*tasks)
        start += chunk
        tasks = []


if __name__ == '__main__':
    asyncio.run(main())
