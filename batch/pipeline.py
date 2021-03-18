"""
1. Provide the following information for each card: order date, issue date,
activation date, first used date.

2. Provide the list of cards which took more than 3 days to be activated and
more than 10 days to be used for the first time. Include the corresponding
dates.

3. Provide the list of canceled cards and how many days each of them was
active.

4. Provide the list of merchants together with the number of transactions they
had.

5. Provide the transaction counts by status and by merchant.

6. Provide the list of top 10 highest spending cards.
"""
# card
[
    'id',
    'current_instance_id',
    'creation_date',
    'last_update',
    'provider_card_id',
    'provider_id',
    'user_id',
    'status',
    'paused',
    'type',
    'subtype',
    'currency',
    'name',
    'brand',
    'program',
    'provider_version',
    'multicurrency',
]

# card instance
[
    'id',
    'card_id',
    'creation_date',
    'last_update',
    'provider_card_id',
    'design',
    'activation_date',
    'deactivation_date',
    'deactivation_reason',
    'shipping_address_id',
    'pin_set_date',
    'pin_id',
    'last_pan_digits',
]

# card_instance_shipping
# In [115]: card_instance_shipping
# Out[115]:
#            id card_instance_id       creation_date              status  delivery_estimate_days
# 0     477,058          198,959 2020-01-02 13:52:24  delivery_confirmed                      14
# 1     477,062          199,201 2020-01-02 19:48:12  delivery_confirmed                      14
# 2     477,066          199,231 2020-01-03 15:32:21             shipped

# In [112]: card_instance[card_instance['card_id'] == '110,693' ]
# Out[112]:
#            id  card_id       creation_date         last_update provider_card_id          design activation_date   deactivation_date deactivation_reason shipping_address_id pin_set_date pin_id  last_pan_digits
# 1838  199,549  110,693 2020-01-13 01:37:32 2020-04-01 14:24:50         110693_1  black_physical             NaT 2020-04-01 14:24:50       reported_lost                None          NaT   None              NaN
# 4248  202,753  110,693 2020-04-01 14:24:50 2020-07-20 08:41:06         110693_2  black_physical             NaT 2020-07-22 13:20:46  program_terminated                None          NaT   None              NaN

# In [114]: card[card['id'] == '110,693' ]
# Out[114]:
#           id current_instance_id       creation_date         last_update provider_card_id  provider_id    user_id  status  paused   type  subtype currency         name brand          program  provider_version  multicurrency
# 336  110,693             202,753 2020-01-13 01:37:30 2020-07-20 08:41:06         110693_2            5  9,900,592  CLOSED       0  debit  PLASTIC      BRL  Cartão Xapo   elo  debit.br.ewally                 1              0


import os
from pathlib import Path
import pyspark
import pandas as pd
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
import pyspark.sql.functions as F
from typing import List

RAW_DATA_DIR = Path('./code/data')


spark = (
    pyspark.sql.SparkSession.builder.enableHiveSupport()
    .config('spark.sql.warehouse.dir', '/home/jovyan/spark-warehouse2')
    .appName('Xapo')
    .getOrCreate()
)


def load_csv(csv: os.PathLike) -> pyspark.sql.DataFrame:
    """
    Load a CSV file as DataFrame
    """
    return spark.read.csv(
        str(csv),
        header=True,
        sep=',',
        inferSchema=True,
        timestampFormat='yyyy-MM-dd HH:mm:ss',
        mode='FAILFAST',
    )


def create_view(df: pyspark.sql.DataFrame, table_name) -> None:
    df.createOrReplaceTempView(table_name)


def str_to_int(df: pyspark.sql.DataFrame, columns: List[str]) -> pyspark.sql.DataFrame:
    ...


def load_query(path: os.PathLike) -> str:
    with open(path, 'r') as f:
        return f.read()


def usd_rates():
    import json
    from typing import List
    from urllib.request import urlopen

    ENDPOINT = 'https://api.exchangeratesapi.io/latest/'

    def build_url(base: str, symbols: List[str]) -> str:
        symbols = ','.join(symbols)
        return f'{ENDPOINT}?base={base}&{symbols}'

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
    return spark.createDataFrame(usd_rates.items(), ['symbol', 'dollar'])


def main():

    for csv in RAW_DATA_DIR.glob('*.csv'):
        name = csv.stem
        df = load_csv(csv)
        create_view(df, name)

    create_view(usd_rates(), 'usd_rates')
    card_info = spark.sql(load_query('code/queries/card_info.sql'))
    create_view(card_info, 'card_info')

    # return spark.sql(load_query('code/queries/tx_per_merchant.sql'))
    # return spark.sql(load_query('code/queries/canceled_cards.sql'))
    # return spark.sql(load_query('code/queries/top10_cards.sql'))
    return spark.sql(load_query('code/queries/card_usage.sql'))


if __name__ == '__main__':
    # card = load_csv(RAW_DATA_DIR / 'card.csv').toPandas()
    # card_instance = load_csv(RAW_DATA_DIR / 'card_instance.csv').toPandas()
    # card_instance_shipping = load_csv(RAW_DATA_DIR / 'card_instance_shipping.csv').toPandas()
    # transactions = load_csv(RAW_DATA_DIR / 'transactions.csv').toPandas()
    df = main().toPandas()
    print(df)
