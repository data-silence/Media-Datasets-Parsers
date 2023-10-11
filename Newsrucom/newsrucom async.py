"""
Scraper for asynchronous news gathering from newsru.com
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

date_dict = \
    {
        "июня": 6, "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12, "января": 1,
        "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "february": 2, "january": 1, "march": 3, "april": 4, "may": 5,
        "june": 6, "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
    }

logger.add('logs/newsrucom.json', format="{time} {message}", level='ERROR', rotation="1 week",
           compression="zip",
           serialize=True)
def get_url_list(date):
    """Takes a list of links for news scraping on a specific date"""
    agency_url = 'https://www.newsru.com/allnews/' + str(date.strftime("%d%b%Y").lower())
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}
    answer = requests.get(agency_url, headers=headers)
    try:
        if answer and answer.status_code != 204:
            soup = BeautifulSoup(answer.text, features="html.parser")
            paragraph = soup.body.find(attrs={'class': 'content-main'}).find_all(attrs={'class': 'inner-news-item'})
            links = (tuple('https://www.newsru.com' + el.a.get('href') for el in paragraph if not el.a.get('href').startswith('http')))
        else:
            links = tuple()
    except AttributeError:
        logger.error(date)
        links = tuple()

    return links


async def write_to_db(url, date, news, agency, img_url, title, links):
    """Writes the news to the database"""
    try:
        conn = await asyncpg.connect(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')
        await (conn.fetch
            (
            'INSERT INTO newsrucom (url, date, news, agency, img_url, title, links) '
            'VALUES ($1, $2, $3, $4, $5, $6, $7)',
            url, date, news, agency, img_url, title, links
        )
        )
        await conn.close()
    except Exception:
        logger.error(url)

    await asyncio.sleep(0.3)


async def fetch_content(url, session):
    """Collects the necessary attributes of a news item on its page"""
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}

    async with session.get(url=url, headers=headers) as response:
        if response and response.status != 204:
            answer = await response.text()
            soup = BeautifulSoup(answer, features="html.parser")
            news_fields = soup.find(attrs={'class': 'article'})

            # title
            title = news_fields.h1.text.replace('╚', '`').replace('╩', '`')


            # news
            draft_news = news_fields.find(attrs={'class': 'article-text'}).p.text
            draft_news = draft_news.replace('╚', '`').replace('╩', '`').replace('\r', '').replace('\n', '').replace('√', '-').strip()
            news_list = draft_news.split('.')
            news_list = [news.strip() for news in news_list if news]
            news = '. '.join(news_list)

            # date
            date_string = news_fields.find(attrs={'class': 'article-date'}).text.split('|')[0].split('время публикации:')[
                -1].strip()
            rus_month = date_string.split()[1]
            date_string = date_string.replace(rus_month, str(date_dict[rus_month]))
            date = dt.datetime.strptime(date_string, "%d %m %Y г., %H:%M")

            # img_url
            try:
                img_url = news_fields.find(attrs={'class': 'article-img'})['src']
            except TypeError:
                img_url = ''
                # logger.error('Фотка не найдена, ссылка на неё не сохранена')

            # links
            try:
                articles_block = soup.find(attrs={'class': 'article-list-link'})
                links_set = {el.get('href') for el in articles_block.find_all('a') if
                             el.get('href') and not el.get('href').endswith('/')}
                links_list = ['https://www.newsru.com' + el if el.startswith('/') and el.endswith('html') else el for el in
                              links_set]
                links = [el for el in links_list if not el.startswith('/')]
            except AttributeError:
                links = []

            await write_to_db(url=url, date=date, news=news, agency='newsru.com', img_url=img_url, title=title, links=links)
        else:
            logger.error(url)


async def main():
    """Collecting news for a given time period: get a list of links to news for a date and pass the pages to parser"""
    finish_date = dt.date(2021, 6, 1)
    # finish_date = dt.date(2002, 1, 1)
    current_date = dt.date(2004, 6, 10)
    delta = dt.timedelta(days=1)
    while current_date < finish_date:
        links = get_url_list(date=current_date)
        if links:
            try:
                async with aiohttp.ClientSession(trust_env=True) as session:
                    tasks = []
                    chunk = 50
                    times = len(links) // chunk
                    start = 0
                    for el in range(times + 1):
                        for url in links[start:start + chunk]:
                            task = asyncio.create_task(fetch_content(url, session))
                            tasks.append(task)
                        start += chunk
                        await asyncio.gather(*tasks)
                        await asyncio.sleep(0.7)
                logger.info(current_date)
                current_date += delta
            except AttributeError:
                logger.error(current_date)
        else:
            logger.error(current_date)


if __name__ == '__main__':
    asyncio.run(main())
