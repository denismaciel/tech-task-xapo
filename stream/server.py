"""
Socket server that receives transaction information from worker and processes
it.

For simplicity, the processing consists of only printing some stats about
transactions fees to the terminal.
"""
import json
import socket

HOST = '127.0.0.1'
PORT = 65432
END = b'_end'


def server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((HOST, PORT))
    sock.listen()

    while True:
        client, addr = sock.accept()
        print('Connection', addr)
        handler(client)


def handler(client):
    """
    Handles every new incoming connection.
    """
    data = b''
    while True:
        new = client.recv(1024)
        if new[-4:] == END:
            data += new.replace(END, b'')
            break
        data += new
    txs = json.loads(data.decode('utf-8'))
    client.send(json.dumps({'total_processed': len(txs)}).encode())
    process(txs)


def process(txs):
    """
    Print transactoin fees stats to the terminal.
    """
    n = len(txs)
    block_height = txs[0]['block_height']
    total_fees = sum(tx['fee'] for tx in txs)

    print(
        'Received {} from block {}. Average fees were {}'.format(
            n, block_height, round(total_fees / n)
        )
    )


if __name__ == '__main__':
    server()
