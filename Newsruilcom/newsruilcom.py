"""
Scraper for asynchronous news gathering from newsru.co.il
Used for non-commercial research purposes
"""

import asyncio
import aiohttp
import asyncpg

import datetime as dt
import requests
from bs4 import BeautifulSoup
import random
from loguru import logger


def get_url_list(date):
    """Takes a list of links for news scraping on a specific date"""
    agency_url = 'https://www.newsru.co.il/world/' + str(date.strftime("%d%b%Y").lower())
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}
    answer = requests.get(agency_url, headers=headers)
    soup = BeautifulSoup(answer.text, features="html.parser")
    paragraph = soup.body.section.find(attrs={'class': 'topic-list-column'}).find_all(
        attrs={'class': 'topic-list-container'})
    links = tuple(text.find(attrs={'class': 'index-news-image'}).a.get('href') for text in paragraph)
    return links


async def write_to_db(url, date, news, agency, img_url, title):
    """Writes the news to the database"""
    conn = await asyncpg.connect(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')
    await conn.fetch('INSERT INTO news (url, date, news, agency, img_url, title) VALUES ($1, $2, $3, $4, $5, $6)', url,
                     date, news, agency, img_url, title)
    await conn.close()
    await asyncio.sleep(0.5)


async def fetch_content(url, session):
    """Collects the necessary attributes of a news item on its page"""
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}

    async with session.get(url=url, headers=headers) as response:
        answer = await response.text()
        soup = BeautifulSoup(answer, features="html.parser")
        news_fields = soup.find(attrs={'class': 'main-news-body height-auto bg-white'})

        # title
        title = news_fields.h1.text

        # news
        draft_news = news_fields.find(attrs={'class': 'text'}).text
        draft_news = draft_news.replace("\nadv_12 incontent_1\n\n \nadv_13 m_ar_1\n\n", '').replace("\n", '').strip()
        news_list = draft_news.split('.')
        news_list = [news.strip() for news in news_list if news]
        news = '. '.join(news_list)

        # date
        date_field = news_fields.find(attrs={'class': 'info'})
        unix_timestamp = date_field.find_all('time')[-1]['datetime']
        date = dt.datetime.fromtimestamp(int(unix_timestamp))

        # img_url
        try:
            img_url = news_fields.find(attrs={'class': 'pointer newsImg'})['src']
        except TypeError:
            img_url = ''
            logger.error('Фотка не найдена, ссылка на неё не сохранена')

        await write_to_db(url=url, date=date, news=news, agency='newsru.co.il', img_url=img_url, title=title)


async def main():
    """Collecting news for a given time period: get a list of links to news for a date and pass the pages to parser"""
    current_date = dt.date(2021, 5, 29)
    finish_date = dt.date(2023, 1, 1)
    delta = dt.timedelta(days=1)
    while current_date < finish_date:
        links = get_url_list(date=current_date)
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in links:
                task = asyncio.create_task(fetch_content(url, session))
                tasks.append(task)
            await asyncio.gather(*tasks)
        logger.info(current_date)
        current_date += delta


if __name__ == '__main__':
    asyncio.run(main())
