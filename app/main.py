# Uncomment this to pass the first stage
import socket
from concurrent.futures import ThreadPoolExecutor
import signal
import sys

import app.reply
#import parse

#print(app.reply.parse(b"+PING\r\n"))

store = {}
def reply(c):
    while True:
        payload = c.recv(1024)
        if not payload:
            break
        l = app.reply.parse(payload)
        print(l)
        l[0] = l[0].upper()
        if l[0] == b'PING':
            c.send(b"+PONG\r\n")
        elif l[0] == b'ECHO':
            c.send(app.reply.bulk_string(l[1]))
        elif l[0] == b'SET':
            store[l[1]]=l[2]
            c.send(b"+OK\r\n")
        elif l[0] == b'GET':
            if not l[1] in store:
                c.send(b"$-1\r\n")
            else:                
                c.send(app.reply.bulk_string(store[l[1]]))
    print("Client disconnected")


#def sigint(signum, frame):
#    print(f'handlerが呼び出されました(signum={signum})')
#    sys.exit(0)

#signal.signal(signal.SIGINT, sigint)
signal.signal(signal.SIGINT, signal.SIG_DFL)


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    tpe = ThreadPoolExecutor(max_workers=3)
    while True:
        c, _ = server_socket.accept()  # wait for client
        tpe.submit(reply, c)


if __name__ == "__main__":
    main()
