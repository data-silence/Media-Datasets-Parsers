import torch
from loguru import logger
from transformers import MBartTokenizer, MBartForConditionalGeneration

import asyncio
import asyncpg

device = "cuda" if torch.cuda.is_available() else "cpu"

DB_USER = pass
DB_NAME = pass
DB_PASS = pass
DB_HOST = pass

# Параметры подключения к базе данных PostgreSQL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# Размер батча для обработки модели суммаризации
BATCH_SIZE = 16


def get_model(model_name: str = "IlyaGusev/mbart_ru_sum_gazeta") -> tuple:
    model_params = {
        "IlyaGusev/mbart_ru_sum_gazeta":
            {
                'tokenizer': MBartTokenizer,
                'model': MBartForConditionalGeneration,
                "column_name": 'news',
                'result_column': 'resume'
            },
    }

    tokenizer = model_params[model_name]['tokenizer'].from_pretrained(model_name)
    model = model_params[model_name]['model'].from_pretrained(model_name)
    model.to(device)
    # column_name = model_params[model_name]['column_name']
    # result_column = model_params[model_name]['result_column']
    return tokenizer, model


def summarize_batch(news_texts) -> list:
    input_ids = tokenizer(
        news_texts,
        max_length=600,
        truncation=True,
        padding=True,
        return_tensors="pt", )["input_ids"].to(device)

    output_ids = model.generate(
        input_ids=input_ids,
        no_repeat_ngram_size=4)

    summary = tokenizer.batch_decode(output_ids, skip_special_tokens=True)

    # logger.info(f'Обработка завершена успешно')
    return summary


async def get_news_without_resume(conn):
    query = """
        SELECT url, news 
        FROM lenta
        WHERE resume IS NULL
        LIMIT $1
    """
    return await conn.fetch(query, BATCH_SIZE)


async def update_news_resume(conn, news_resumes):
    query = """
        UPDATE lenta
        SET resume = $1
        WHERE url = $2
    """
    await conn.executemany(query, news_resumes)


async def process_news_batch(pool):
    async with pool.acquire() as conn:
        news_batch = await get_news_without_resume(conn)
        news_texts = [row['news'] for row in news_batch]
        resumes = summarize_batch(news_texts)
        news_resumes = [(resume, row['url']) for resume, row in zip(resumes, news_batch)]
        await update_news_resume(conn, news_resumes)


async def main():
    pool = await asyncpg.create_pool(DATABASE_URL)
    while True:
        i = 1
        logger.info(f'Начинается обработка {i} транша')
        await process_news_batch(pool)
        await asyncio.sleep(10)  # Задержка между итерациями
        logger.info(f'На текущий момент обработано {i * BATCH_SIZE} новостей')
        i += 1
    await pool.close()

