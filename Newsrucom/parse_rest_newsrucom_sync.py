from imports_shop import *
from db import *


time_machine = create_engine(
    f'postgresql+psycopg2://{DB_NAME}:{DB_PASS}@{DB_HOST}/{DB_NAME}', pool_pre_ping=True)


def get_list():
    q = f"select url from news where url like 'https://txt.newsru.com/%' and (news = 'NaN' or news = '')"
    result = DataBaseMixin.get(q, time_machine)
    news_list = [el['url'] for el in result]
    return news_list


def fetch_content(url):
    user_agents = open('proxy/user-agents.txt').read().splitlines()
    random_user_agent = random.choice(user_agents)
    headers = {'User-Agent': random_user_agent}
    answer = requests.get(url, headers=headers)
    try:
        soup = BeautifulSoup(answer.text, features="html.parser")
        news_list = [p.text for p in soup.body.find('div', attrs='article-text').find_all('p')]
        news = ''.join(news_list).replace("'", "`").strip()
        print(news)
        with time_machine.begin() as conn:
            conn.execute(text(f"""UPDATE news SET news='{news}' WHERE url='{url}'"""))
    except (IndexError, AttributeError):
        logger.error(f'{url}')


def main():
    logger.info('Делаю запрос к базе для получения списка необработанных новостей')
    news_list = get_list()
    for i, url in enumerate(news_list):
        logger.info(f'{i+1}/{len(news_list)} {url} - идёт обработка...')
        fetch_content(url)


if __name__ == '__main__':
    main()
