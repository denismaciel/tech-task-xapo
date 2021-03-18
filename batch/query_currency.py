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
print(usd_rates)
