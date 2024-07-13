from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import os
import logging

# MongoDb
mongo_uri = os.getenv('MONGO_URI', 'mongodb://192.168.3.201:27017/')

# Postgres
pg_uri = os.getenv(
    'POSTGRES_URI', 'postgresql://root:dietpi@192.168.3.200/articles')


def get_dataframe_from_mongo(client):
    news_db = client['news_db']
    articles = news_db['articles']

    nasdaq_index = client['nasdaq_index']
    ixic = nasdaq_index['ixic']

    articles_data = list(articles.find())
    ixic_data = list(ixic.find())

    news_df = pd.DataFrame(articles_data)
    ixic_df = pd.DataFrame(ixic_data)

    return news_df, ixic_df


def transform_date(news_df, ixic_df):
    news_df['publishedAt'] = news_df['publishedAt'].dt.date
    ixic_df['Date'] = ixic_df['Date'].dt.date

    return news_df, ixic_df


def fill_dates(ixic_df):
    df = ixic_df
    df.set_index('Date', inplace=True)

    df = df[~df.index.duplicated(keep='first')]

    date_range = pd.date_range(start=df.index.min(), end=df.index.max())
    df = df.reindex(date_range)

    df = df.ffill()
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Date'}, inplace=True)

    df['Date'] = df['Date'].dt.date

    return df


def remove_columns_and_normalize_source(df):
    json_df = pd.json_normalize(df['source'])
    json_df['source_id'] = json_df['id']
    json_df['source_name'] = json_df['name']
    json_df = json_df.drop('id', axis=1)
    json_df = json_df.drop('name', axis=1)

    dropped_df = pd.concat([df, json_df], axis=1)
    dropped_df = df.drop(
        columns=['_id_x', '_id_y', '__v', 'source', 'publishedAt'])

    return dropped_df


def rename_columns(df):
    schema = {
        'urlToImage': 'url_to_image',
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume',
        'Dividends': 'dividends',
        'Stock Splits': 'stock_splits',
    }
    df = df.rename(columns=schema)

    return df


def save_to_pg(df):
    engine = create_engine(pg_uri)
    df.to_sql(name='refined_news', con=engine,
              if_exists='replace', index=True)


def main():
    logger = logging.getLogger(__name__)
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    client = MongoClient(mongo_uri)

    logger.info('Getting data from Mongodb')
    news_df, ixic_df = get_dataframe_from_mongo(client)
    logger.info('Got data from Mongodb')

    logger.info('Processing data')

    news_df, ixic_df = transform_date(news_df, ixic_df)

    ixic_df = fill_dates(ixic_df)

    df = pd.merge(news_df, ixic_df, left_on='publishedAt',
                  right_on='Date', how='inner')

    df = remove_columns_and_normalize_source(df)

    df = rename_columns(df)

    df = df.set_index('date')

    logger.info('Data processing was succesfully')

    logger.info('Saving data to postgres')

    save_to_pg(df)

    logger.info('Saved data to postgres')

    return 'OK'


if __name__ == '__main__':
    main()
