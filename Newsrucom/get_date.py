import asyncio
import aiohttp
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import random

import pandas as pd
from sqlalchemy import create_engine


arh_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')

news_df = pd.read_sql(f"SELECT * FROM news", arh_engine)

my_dates = {}
error_dates = {}


async def get_page_data(session, news_url, date):
	user_agents = open('proxy/user-agents.txt').read().splitlines()
	random_user_agent = random.choice(user_agents)
	headers = {'User-Agent': random_user_agent}
	async with session.get(news_url, headers) as response:
		response_text = await response.text()

		try:
			soup = BeautifulSoup(response_text, 'lxml-xml').body.find('div', attrs='article-date')
			raw_date = soup.text.strip('\n').strip(' ').strip().split(',', 2)
			str_time = raw_date[1].split('\n', 1)[0].strip().split(' ')[0]
			full_str_time = str(date) + ' ' + str_time
			form_date = datetime.strptime(full_str_time, "%Y-%m-%d %H:%M")
			print(f'{news_url} успешно')
		except AttributeError:
			full_str_time = str(date) + ' ' + '12:00'
			form_date = datetime.strptime(full_str_time, "%Y-%m-%d %H:%M")
			error_dates[news_url] = form_date
			print(f'{news_url} косяк')

		return form_date


def main():
	asyncio.run()
	news_df['timestamp'] = news_df.apply(lambda x: get_page_data(x[0], x[5]), axis=1)


if __name__ == "__main__":
	main()
