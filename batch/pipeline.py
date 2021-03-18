import shutil
import json
import os
from pathlib import Path
from typing import List
from urllib.request import urlopen

import prefect
import pyspark

RAW_DATA_DIR = Path('./data')
QUERIES_DIR = Path('./queries')


session = (
    pyspark.sql.SparkSession.builder.enableHiveSupport().appName('Xapo').getOrCreate()
)


@prefect.task
def get_spark_session():
    return (
        pyspark.sql.SparkSession.builder.enableHiveSupport()
        .appName('Xapo')
        .getOrCreate()
    )


def load_csv(session, csv: os.PathLike) -> pyspark.sql.DataFrame:
    """
    Load a CSV file as a Spark DataFrame
    """
    return session.read.csv(
        str(csv),
        header=True,
        sep=',',
        inferSchema=True,
        timestampFormat='yyyy-MM-dd HH:mm:ss',
        mode='FAILFAST',
    )


def create_view(df: pyspark.sql.DataFrame, table_name: str) -> None:
    """
    Allow a Spark DataFrame to be reference in a SQL query.
    """
    df.createOrReplaceTempView(table_name)


def load_query(path: os.PathLike) -> str:
    with open(path, 'r') as f:
        return f.read()


def usd_rates(session):
    """
    Query latest exchange rate between USD and `symbols`.
    """

    ENDPOINT = 'https://api.exchangeratesapi.io/latest/'

    def build_url(base: str, symbols: List[str]) -> str:
        symbols_str = ','.join(symbols)
        return f'{ENDPOINT}?base={base}&{symbols_str}'

    symbols = [
        'GBP',
        'EUR',
        'BRL',
        'RON',
        'CHF',
        'COP',
        'USD',
        'PLN',
        'ARS',
        'CZK',
        'UYU',
        'BGN',
        'HUF',
        'NOK',
        'HRK',
        'AED',
        'SEK',
        'DOP',
        'SGD',
        'INR',
    ]

    url = build_url('USD', symbols)

    with urlopen(url) as response:
        content = response.read().decode('utf-8')

    usd_rates = json.loads(content)['rates']
    return session.createDataFrame(usd_rates.items(), ['symbol', 'dollar'])

def query_results(table_name):
    return session.sql(f'SELECT * FROM {table_name} LIMIT 10').toPandas()

# =======================
# ==== Prefect tasks ====
# =======================

@prefect.task
def task_create_view_from_csv(session):
    for csv in RAW_DATA_DIR.glob('*.csv'):
        df = load_csv(session, csv)
        create_view(df, table_name=csv.stem)


@prefect.task
def task_create_view_usd_rates(session):
    df = usd_rates(session)
    create_view(usd_rates(session), 'usd_rates')


@prefect.task
def run_query(session, file_name):
    df = session.sql(load_query(file_name))
    create_view(df, Path(file_name).stem)


def orchestrate():
    with prefect.Flow('batch-pipeline') as flow:
        csv_import = task_create_view_from_csv(session)
        usd_rates = task_create_view_usd_rates(session)

        card_usage = run_query(session, 'queries/card_usage.sql')
        card_info = run_query(session, 'queries/card_info.sql')
        tx_per_merchant = run_query(session, 'queries/tx_per_merchant.sql')
        canceled_cards = run_query(session, 'queries/canceled_cards.sql')
        top10_cards = run_query(session, 'queries/top10_cards.sql')

        card_info.set_dependencies(upstream_tasks=[csv_import])
        card_usage.set_dependencies(upstream_tasks=[csv_import, card_info])
        tx_per_merchant.set_dependencies(upstream_tasks=[csv_import])
        canceled_cards.set_dependencies(upstream_tasks=[csv_import])
        top10_cards.set_dependencies(upstream_tasks=[csv_import, usd_rates])
    flow.run()


def _main():
    session = get_spark_session()
    for csv in RAW_DATA_DIR.glob('*.csv'):
        name = csv.stem
        df = load_csv(session, csv)
        create_view(df, name)

    create_view(usd_rates(session), 'usd_rates')
    card_info = session.sql(load_query('queries/card_info.sql'))
    create_view(card_info, 'card_info')

def _center_text(text: str) -> str:
    width, height = shutil.get_terminal_size()
    return f"   {text}   ".center(width, "=")

if __name__ == '__main__':
    orchestrate()
    for query in QUERIES_DIR.glob('*.sql'):
        table_name = query.stem
        print('\n\n')
        print(_center_text(f'{table_name}'))
        print(query_results(table_name))
