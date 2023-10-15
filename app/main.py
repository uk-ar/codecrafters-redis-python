import socket
from concurrent.futures import ThreadPoolExecutor
import signal
import select
import sys
sys.path.append("../")
from app.reply import reply,config

def handle(c):
    payload = c.recv(1024)
    if not payload:
        print("Client disconnected")
        return False
    #print(payload)
    resp = reply(payload)
    #print(resp)
    c.send(resp)
    return True

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

    def listen_epoll(self):
        epoll = select.epoll()
        self.server.setblocking(False)
        # select.EPOLLIN: ready to read
        # select.EPOLLET: edge triggered
        epoll.register(self.server, select.EPOLLIN | select.EPOLLET)
        clients = {}
        try:
            while True:
                events = epoll.poll()
                for fd,event in events:
                    if fd == self.server.fileno():
                        con, addr = self.server.accept()
                        print("New connection from", addr)
                        con.setblocking(False)
                        epoll.register(con, select.EPOLLIN | select.EPOLLET)
                        clients[con.fileno()]=con
                    elif event & select.POLLIN:
                        if not handle(clients[fd]):
                            epoll.unregister(fd)
                            clients[fd].close()
                            del clients[fd]
        finally:
            for _,con in clients:
                epoll.unregister(con)
                con.close()
            epoll.unregister(self.server)
            epoll.close()
            self.server.close()
            # TODO close all clients?

if __name__ == "__main__":
    s = RedisServer()
    s.listen_epoll()
