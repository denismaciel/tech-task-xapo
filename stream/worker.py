"""

"""
import itertools
import json
import logging
import time
import urllib
from typing import Any
from typing import Dict
from urllib.request import urlopen

import base58


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)


def query_api(page):
    ENDPOINT = f'https://chain.api.btc.com/v3/block/latest/tx?page={page}'

    try:
        with urlopen(ENDPOINT) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        # In case of 429 (too many requests error), wait longer and call the
        # function again.
        if e.code == 429:
            log.info(
                'HTTP Error 429: too many requests.'
                'Sleeping for 1 minute before restarting API calls.'
            )
            time.sleep(60)
            return query_api(page)
        else:
            raise e


def is_valid(address: str) -> bool:
    try:
        base58.b58decode_check(address)
    except ValueError:
        return False
    return True


assert is_valid('15282N4BYEwYh3j1dTgJu64Ey5qWn9Po9F')
assert is_valid('5282N4BYEwYh3j1dTgJu64Ey5qWn9Po9F') is False
assert is_valid('not_a_Bitcoin_address') is False


def extract_valid_addresses(tx) -> Dict[str, Any]:
    inp_addresses = itertools.chain.from_iterable(
        inp['prev_addresses'] for inp in tx['inputs']
    )
    out_addresses = itertools.chain.from_iterable(
        out['addresses'] for out in tx['outputs']
    )
    return {
        'input_addresses': [addr for addr in inp_addresses if is_valid(addr)],
        'output_addresses': [addr for addr in out_addresses if is_valid(addr)],
    }


def format_tx(tx: Dict) -> Dict:
    return {
        'fee': tx['fee'],
        'block_time': tx['block_time'],
        'block_height': tx['block_height'],
        'transaction_value': tx['outputs_value'] + tx['fee'],
        **extract_valid_addresses(tx),
    }


def extract_txs(response):
    raw_txs = response['data']['list']
    txs = [format_tx(tx) for tx in raw_txs]
    return txs


def has_block_been_seen(response, seen):
    return response['data']['list'][0]['block_height'] in seen


def current_page(response):
    return int(response['data']['page'])


def total_pages(response):
    return int(response['data']['page_total'])


def txs_belong_to_same_block(txs):
    block_heights = [tx['block_height'] for tx in txs]
    first, *_ = block_heights
    return all(block == first for block in block_heights)


assert txs_belong_to_same_block(
    [
        {'block_height': 30},
        {'block_height': 30},
        {'block_height': 30},
    ]
)
assert (
    txs_belong_to_same_block(
        [
            {'block_height': 30},
            {'block_height': 35},
            {'block_height': 30},
        ]
    )
    is False
)


def main():
    """
    Contains the logic of the worker.
    """
    WAIT_FOR_NEW_PAGE = 3
    WAIT_FOR_NEW_BLOCK = 60 * 5
    SEEN_BLOCKS = set()
    page = 1

    while True:
        response = query_api(page)
        total = total_pages(response)
        log.info(f'Querying page: {page} of {total}')
        assert page == current_page(response)

        if current_page(response) == 1 and has_block_been_seen(response, SEEN_BLOCKS):
            time.sleep(WAIT_FOR_NEW_BLOCK)
            log.info(f'Seen blocks: {SEEN_BLOCKS}')
            continue

        log.info(f'Seen blocks so far: {SEEN_BLOCKS}')
        txs = extract_txs(response)
        # As far as I can tell, the API provides paginated data about the
        # latest block
        assert txs_belong_to_same_block(txs)
        SEEN_BLOCKS.add(txs[0]['block_height'])

        # If processing last page, reset page counter. Otherwise, go to next
        # page.
        if current_page(response) == total:
            page = 1
        else:
            page += 1

        time.sleep(WAIT_FOR_NEW_PAGE)


if __name__ == '__main__':
    main()
