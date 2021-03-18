"""
Worker responsible for querying the API endpoint at regular intervals,
formatting and transimiting transaction information to the server.
"""
import itertools
import json
import logging
import socket
import time
import urllib
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
    """
    Check if `address` is a valid Bitcoin address.
    """
    try:
        base58.b58decode_check(address)
    except ValueError:
        return False
    return True


assert is_valid('15282N4BYEwYh3j1dTgJu64Ey5qWn9Po9F')
assert is_valid('5282N4BYEwYh3j1dTgJu64Ey5qWn9Po9F') is False
assert is_valid('not_a_Bitcoin_address') is False


def extract_valid_addresses(tx):
    """
    Extract input and output addresses from the dictionary representing a
    transaction.
    """
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


def format_tx(tx):
    """
    Reformat and extract relevant information from dictionary representing a
    transaction.
    """
    return {
        'fee': tx['fee'],
        'block_time': tx['block_time'],
        'block_height': tx['block_height'],
        'transaction_value': tx['outputs_value'] + tx['fee'],
        **extract_valid_addresses(tx),
    }


def extract_txs(response):
    """
    Extract and format transactions from API response.
    """
    raw_txs = response['data']['list']
    txs = [format_tx(tx) for tx in raw_txs]
    return txs


def has_block_been_seen(response, seen):
    """
    Check if block has been processed already.
    """
    return response['data']['list'][0]['block_height'] in seen


def current_page(response):
    return int(response['data']['page'])


def total_pages(response):
    return int(response['data']['page_total'])


def txs_belong_to_same_block(txs):
    """
    Check the assumption that all transactions in an API response belong to the
    same block.
    """
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


def add_block_to_seen(txs, seen):
    seen.add(txs[0]['block_height'])


def send(data):
    """
    Send data to the server and parse response.
    """
    HOST = '127.0.0.1'
    PORT = 65432

    def encode_data(d: Dict) -> bytes:
        return json.dumps(d).encode()

    encoded = encode_data(data)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(encoded)
        s.send(b'_end')  # Signal to server that all data has been transferred
        response = s.recv(1024)

    return json.loads(response.decode('utf-8'))


def main():
    """
    Define logic for querying the API and processing reponse data.
    """
    WAIT_FOR_NEW_PAGE = 3
    WAIT_FOR_NEW_BLOCK = 60 * 5
    seen_blocks = set()
    page = 1

    while True:
        response = query_api(page)
        total = total_pages(response)
        log.info(f'Querying page: {page} of {total}')
        assert page == current_page(response)

        # If clause runs if current block has been already processed, in which
        # case Wait longer for a new block to added to the blockchain.
        if current_page(response) == 1 and has_block_been_seen(response, seen_blocks):
            time.sleep(WAIT_FOR_NEW_BLOCK)
            log.info(f'Seen blocks: {seen_blocks}')
            continue

        log.info(f'Blocks seen so far: {seen_blocks}')
        txs = extract_txs(response)
        # Assumption: transaction in a response must belong to same block.
        assert txs_belong_to_same_block(txs)
        add_block_to_seen(txs, seen_blocks)
        server_resp = send(txs)
        assert server_resp['total_processed'] == len(txs)

        # If last page, reset page counter. Otherwise, go to next
        # page.
        if current_page(response) == total:
            page = 1
        else:
            page += 1

        time.sleep(WAIT_FOR_NEW_PAGE)


if __name__ == '__main__':
    main()
