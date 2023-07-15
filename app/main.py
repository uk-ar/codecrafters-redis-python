import socket
from concurrent.futures import ThreadPoolExecutor
import signal
import select

import app.reply

def handle(c):
    payload = c.recv(1024)
    if not payload:
        print("Client disconnected")
        return
    #print(payload)
    resp = app.reply.reply(payload)
    #print(resp)
    c.send(resp)

signal.signal(signal.SIGINT, signal.SIG_DFL)

class RedisServer:

    def __init__(self):
        self.server = socket.create_server(("localhost", 6379),
                                           reuse_port=True)
        self.client = []

    def listen(self):
        while True:
            fds_to_watch = [self.server, *self.client]
            ready_to_read, _, _ = select.select(fds_to_watch, [], [])
            for ready in ready_to_read:
                if ready == self.server:
                    con, addr = self.server.accept()
                    print("New connection from", addr)
                    self.client.append(con)
                else:
                    handle(ready)

if __name__ == "__main__":
    s = RedisServer()
    s.listen()
