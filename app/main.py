# Uncomment this to pass the first stage
import socket
from concurrent.futures import ThreadPoolExecutor
import signal

import app.reply
#import parse

#print(app.reply.parse(b"+PING\r\n"))

def handle(c):
    while True:
        payload = c.recv(1024)
        if not payload:
            break
        print(payload)
        resp = app.reply.reply(payload)
        print(resp)
        c.send(resp)
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
        tpe.submit(handle, c)


if __name__ == "__main__":
    main()
