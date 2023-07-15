import socket
from concurrent.futures import ThreadPoolExecutor
import signal

import app.reply

def handle(c):
    while True:
        payload = c.recv(1024)
        if not payload:
            break
        #print(payload)
        resp = app.reply.reply(payload)
        #print(resp)
        c.send(resp)
    print("Client disconnected")

signal.signal(signal.SIGINT, signal.SIG_DFL)


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")


    


class RedisServer:
    def __init__(self):
        self.server_socket = socket.create_server(("localhost", 6379),
                                             reuse_port=True)
    def listen(self):
        tpe = ThreadPoolExecutor(max_workers=3)
        while True:
            c, _ = self.server_socket.accept()  # wait for client
            tpe.submit(handle, c)

if __name__ == "__main__":
    s = RedisServer()
    s.listen()
