import json
import socket
from typing import Dict

HOST = '127.0.0.1'
PORT = 65432


def encode_dictionary(d: Dict) -> bytes:
    return json.dumps(d).encode()

def send(data: bytes) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall(data)
        # Signal that all data has been transferred
        s.send(b'_end')
        response = s.recv(1024)

    print('Received', response)

def main():
    ...

if __name__ == "__main__":
    ...
