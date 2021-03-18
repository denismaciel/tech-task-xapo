#!/usr/bin/env python3
import json
import socket

HOST = '127.0.0.1'
PORT = 65432
END = b'_end'

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()

    with conn:
        print('Connected by', addr)
        data = b''
        while True:
            new = conn.recv(1024)
            if new[-4:] == END:
                data += new.replace(END, b'')
                break
            data += new
    print(json.loads(data.decode('utf-8')))
