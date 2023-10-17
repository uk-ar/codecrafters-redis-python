import socket
from concurrent.futures import ThreadPoolExecutor
import signal
import select
import subprocess
import sys
import os
from struct import pack, unpack, calcsize, iter_unpack
sys.path.append("../")
from app.reply import reply, config, store, command_set

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

def length(f) -> int:
    byte = int.from_bytes(f.read(1),byteorder="little")
    if (byte >> 6) == 0b00:
        return byte & 0x3f
    #elif (byte >> 6) == 0b01:   

def read_obj(f) -> int:
    byte = int.from_bytes(f.read(1),byteorder="little")
    if (byte >> 6) == 0b00:
        #return byte
        return f.read(byte & 0x3f)
    elif (byte >> 6) == 0b11:
        shift = byte & 0x3f
        if shift == 3:
            return "not supported(compressed)"
        return int.from_bytes(f.read(1 << shift),byteorder="little")

def read_kv(f) -> dict[str,str]:
    key = read_obj(f)   
    value = read_obj(f)
    return {key:value}

if __name__ == "__main__":
    if len(sys.argv) == 5:
        path = sys.argv[2]+"/"+sys.argv[4]
        #res = subprocess.run(["cp", sys.argv[2]+"/"+sys.argv[4], f"/home/yuuki.linux/codecrafters-redis-python/{sys.argv[4]}"], stdout=subprocess.PIPE)
        #sys.stdout.buffer.write(res.stdout)
        if os.path.exists(path):
            with open(path,"rb") as f:
                magic, version = unpack("5s4s", f.read(9))
                print(magic, version)
                op = unpack("c", f.read(1)) # fa
                print(op, read_kv(f))
                op = unpack("c", f.read(1)) # fa
                print(op, read_kv(f))
                op = unpack("c", f.read(1)) # fe
                print(op, length(f)) # select db
                op = unpack("c", f.read(1)) # fb
                print(op, length(f), length(f)) # resize db
                # fd
                # fc
                value_type = unpack("c",f.read(1)) # type
                kv = read_kv(f)
                print(value_type, kv)
                for k,v in kv.items():
                    command_set(k,v)
                    #store[k] = {"value":v}
                #store |= kv
                op = unpack("c", f.read(1)) # ff
                checksum = byte = int.from_bytes(f.read(8),byteorder="little")
                #while op != 0xff:
                #exit(0)
    #store[b"foo"] = "bar"
    s = RedisServer()
    s.listen_epoll()
