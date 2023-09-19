import psycopg2.errors

from imports_shop import *
from db import *
from psycopg2 import errors


time_machine = create_engine(
    f'postgresql+psycopg2://{DB_NAME}:{DB_PASS}@{DB_HOST}/{DB_NAME}', pool_pre_ping=True)


def get_list():
    q = f"select url from news where news.news = '' and news.url like 'http://www.meddaily.ru/%'"
    result = DataBaseMixin.get(q, time_machine)
    meddaily_list = [el['url'] for el in result]
    return meddaily_list


def fetch_content(url):
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}
    answer = requests.get(url, headers=headers)
    try:
        soup = BeautifulSoup(answer.text, features="html.parser")
        news = soup.body.table.find_all('tr')[1].td.find_all('table')[6].find_all(attrs={'class': 'topic_text'})[
            1].text.replace('\n', '').replace('\t', '').replace("'", "`").strip()

        with time_machine.begin() as conn:
            conn.execute(text(f"""UPDATE news SET news='{news}' WHERE url='{url}'"""))
    except IndexError:
        logger.error(f'{url}')


def main():
    logger.info('Делаю запрос к базе для получения списка необработанных новостей')
    meddaily_list = get_list()
    for i, url in enumerate(meddaily_list):
        logger.info(f'{i+1}/{len(meddaily_list)} {url} - идёт обработка...')
        fetch_content(url)


if __name__ == '__main__':
    main()
