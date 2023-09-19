import asyncio
import aiohttp
from imports_shop import *
import random
from loguru import logger
from time import sleep
import pickle
import asyncpg
from db import *


time_machine = create_engine(
    f'postgresql+psycopg2://{DB_NAME}:{DB_PASS}@{DB_HOST}/{DB_NAME}', pool_pre_ping=True)

final_list = []

def wright_to_pickle(data):
    with open('temp/result_meddaily.pickle', 'wb') as f:
        pickle.dump(data, f)



def get_list():
    q = f"select url from news where news.news = 'NaN' and news.url like 'http://www.meddaily.ru/%'"
    result = DataBaseMixin.get(q, time_machine)
    meddaily_list = [el['url'] for el in result]
    return meddaily_list


async def write_to_db(news, url):
    conn = await asyncpg.connect(f'postgresql://{DB_NAME}:{DB_PASS}@{DB_HOST}/{DB_NAME}')
    await conn.fetch('UPDATE news SET news=$1 WHERE url=$2', news, url)
    logger.info(f'{url} записан успешно')
    await conn.close()
    await asyncio.sleep(1)


async def fetch_content(url, session):
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}


    async with session.get(url=url, headers=headers) as response:
        try:
            answer = await response.text()
            soup = BeautifulSoup(answer.text, features="html.parser")
            news = soup.body.table.find_all('tr')[1].td.find_all('table')[6].find_all(attrs={'class': 'topic_text'})[
                1].text.replace('\n', '').replace('\t', '').strip()
            final_list.append({url: news})



            # await write_to_db(news=news, url=url)
        except (UnicodeDecodeError, IndexError):
            logger.error(f'{url}')





async def main():
    logger.info('Делаю запрос к базе для получения списка необработанных новостей')
    meddaily_list = get_list()

    chunk = 200
    tasks = []
    start = 0
    logger.info('Начинаю сбор и запись новостей')
    for el in range(len(meddaily_list) // chunk):
        async with aiohttp.ClientSession() as session:
            for url in meddaily_list[start:start + chunk]:
                task = asyncio.create_task(fetch_content(url, session))
                tasks.append(task)
                logger.info(f'{url} - записан успешно')

            await asyncio.gather(*tasks)
            logger.info(f'Чанк # {el}/{len(meddaily_list) // chunk} обработан')
        start += chunk
        tasks = []


if __name__ == '__main__':
    asyncio.run(main())
    wright_to_pickle(final_list)

