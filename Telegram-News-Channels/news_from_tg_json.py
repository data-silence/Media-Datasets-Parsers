import pandas as pd
import datetime as dt
import dpath
import re
import torch
import fasttext as fasttext
from loguru import logger

from transformers import MBartTokenizer, MBartForConditionalGeneration, AutoTokenizer, T5ForConditionalGeneration, \
    AutoModel


def get_news(content_list: list) -> str:
    news_list = [el['text'] for el in content_list]
    draft_news = ''.join(news_list)
    draft_news = draft_news.replace(
        '\nДАННОЕ СООБЩЕНИЕ (МАТЕРИАЛ) СОЗДАНО И (ИЛИ) РАСПРОСТРАНЕНО ИНОСТРАННЫМ СРЕДСТВОМ МАССОВОЙ ИНФОРМАЦИИ, '
        'ВЫПОЛНЯЮЩИМ ФУНКЦИИ ИНОСТРАННОГО АГЕНТА, И (ИЛИ) РОССИЙСКИМ ЮРИДИЧЕСКИМ ЛИЦОМ, ВЫПОЛНЯЮЩИМ ФУНКЦИИ '
        'ИНОСТРАННОГО АГЕНТА\n\n',
        '').replace(
        'Во время войны оперативно проверить информацию, которую распространяют даже официальные представители '
        'конфликтующих сторон, невозможно.',
        '').replace('\n', ' ').strip()
    clean_news = re.sub(r"\s{2,}", ' ', draft_news)
    return clean_news


def collect_news_attrs(news_dict: dict) -> tuple:
    url = 'https://t.me/meduzalive/' + str(news_dict['id'])
    date = dt.datetime.fromisoformat(news_dict['date'])
    try:
        links = dpath.get(news_dict, "text/*/href")
    except:
        links = url
    news = get_news(news_dict['text_entities'])
    return url, news, date, links


def get_df(df: pd.DataFrame) -> pd.DataFrame:
    df['url'], df['news'], df['date'], df['links'] = zip(*df['messages'].map(collect_news_attrs))
    df.drop(['name', 'type', 'id', 'messages'], axis=1, inplace=True)
    df.to_pickle('agency_draft_news.pkl')
    return df


def get_model(model_name: str) -> tuple:
    model_params = {"IlyaGusev/mbart_ru_sum_gazeta":
        {
            'tokenizer': MBartTokenizer.from_pretrained(model_name),
            'model': MBartForConditionalGeneration.from_pretrained(model_name),
            "column_name": 'news',
            'result_column': 'resume'
        },
        "IlyaGusev/rut5_base_headline_gen_telegram":
            {
                'tokenizer': AutoTokenizer.from_pretrained(model_name),
                'model': T5ForConditionalGeneration.from_pretrained(model_name), "column_name": 'resume',
                'result_column': 'title'
            }
    }

    tokenizer = model_params[model_name]['tokenizer']
    model = model_params[model_name]['model']
    model.to("cuda")
    column_name = model_params[model_name]['column_name']
    result_column = model_params[model_name]['result_column']
    return tokenizer, model, column_name, result_column


def get_summary(df: pd.DataFrame, model_name: str) -> pd.DataFrame:
    tokenizer, model, column_name, result_column = get_model(model_name)

    batch_size = 16
    start = 0
    parents_list = df[column_name].tolist()
    summary_list = []

    epochs_amount = len(parents_list) // batch_size + 1

    for epoch in range(epochs_amount):
        logger.info(f'Эпоха {epoch + 1}/{epochs_amount}')
        current_list = parents_list[start:start + batch_size]

        input_ids = tokenizer(
            current_list,
            max_length=600,
            truncation=True,
            padding=True,
            return_tensors="pt", )["input_ids"].to("cuda")

        output_ids = model.generate(
            input_ids=input_ids,
            no_repeat_ngram_size=4)

        summary = tokenizer.batch_decode(output_ids, skip_special_tokens=True)
        summary_list.extend(summary)

        start += batch_size
    df[result_column] = summary_list
    df.to_pickle('data/df_summary_compressed.pkl', compression='gzip')
    logger.info(f'Обработка {model_name} завершена успешно')
    return df


def get_category(df: pd.DataFrame) -> pd.DataFrame:
    model_class = fasttext.load_model("models/cat_model.ftz")
    df['category'] = df.resume.apply(lambda x: model_class.predict(x)[0][0].split('__', 2)[-1])
    logger.info(f'Классификация завершена, категории успешно присвоены')
    return df


def make_me_embs(sentences: list) -> list:
    tokenizer = AutoTokenizer.from_pretrained("cointegrated/LaBSE-en-ru")
    model = AutoModel.from_pretrained("cointegrated/LaBSE-en-ru").to("cuda")
    encoded_input = tokenizer(sentences, padding=True, truncation=True, max_length=64, return_tensors='pt').to('cuda')
    with torch.no_grad():
        model_output = model(**encoded_input)
    embeddings = model_output.pooler_output
    embeddings = torch.nn.functional.normalize(embeddings)
    return embeddings.tolist()


def get_embs(df: pd.DataFrame) -> pd.DataFrame:
    list_embs = []
    batch_size = 500
    start = 0
    epochs_amount = len(df) // batch_size + 1

    for epoch in range(epochs_amount):
        logger.info(f'Эпоха {epoch + 1}/{epochs_amount}')
        current_list = df.news[start:start + batch_size].tolist()
        embs = make_me_embs(current_list)
        list_embs.extend(embs)
        start = start + batch_size
    df['embs'] = list_embs
    df.to_pickle('data/super_final_compressed.pkl', compression='gzip')
    logger.info(f'Векторизация завершена, эмбеддинги записаны')
    return df


def main(json_news_file: str) -> pd.DataFrame:
    logger.info(f'Начинается обработка json-массива новостей {json_news_file}')
    df_draft = pd.read_json(json_news_file)
    df = get_df(df_draft)
    logger.info(f'Новости записаны в датафрейм, начинается процесс суммаризации')
    model_names = ['IlyaGusev/mbart_ru_sum_gazeta', 'IlyaGusev/rut5_base_headline_gen_telegram']
    for model_name in model_names:
        df = get_summary(df, model_name=model_name)
    df = get_category(df)
    df = get_embs(df)
    logger.info(
        f'Обработка {json_news_file} успешно завершена, итоговый датафрейм сохранён как data/super_final_compressed.pkl')
    return df


if __name__ == '__main__':
    main('result-meduza-1sthalf-2022.json')
