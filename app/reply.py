import string
import unittest
import io
from datetime import datetime, timedelta

def parseStream(bytes):
    kind = bytes.read(1)
    if kind == b"+" or kind == b"-":
        return bytes.readline().rstrip().decode()
    elif kind == b":":
        return int(bytes.readline().rstrip())
    elif kind == b"$":
        n : int = int(bytes.readline().rstrip())
        return bytes.read(n+2).rstrip() # read \r\n
    elif kind == b"*":
        n: int = int(bytes.readline().rstrip())
        return [parseStream(bytes) for _ in range(n)]
    else:
        return "Error"

def bulk_string(bytes: bytes):
    n = len(bytes)
    return b"$"+str(n).encode()+b"\r\n"+bytes+b"\r\n"

store = {}

def reply(payload: bytes):
    l = parse(payload)
    command = l[0].upper()
    if command == b'PING':
        return b"+PONG\r\n"
    elif command == b'ECHO':
        return bulk_string(l[1])
    elif command == b'SET':
        store[l[1]] = {b"value": l[2]}
        if len(l) > 3 and l[3].upper() == b'PX':
            store[l[1]][b"expire"] = datetime.now() + timedelta(milliseconds=int(l[4]))
        else:
            store[l[1]][b"expire"] = datetime.max
        return b"+OK\r\n"
    elif command == b'GET':
        if (not l[1] in store) or (datetime.now() > store[l[1]][b"expire"]):
            return b"$-1\r\n"
        else:
            return bulk_string(store[l[1]][b"value"])

def parse(bytes: bytes):
    """
    >>> parse(b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n')
    [b"ECHO",b"hey"]
    """
    return parseStream(io.BytesIO(bytes))

class TestFunc(unittest.TestCase):
    def test_echo(self):
        self.assertEqual(reply(b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"),
                         b"$3\r\nhey\r\n")

    def test_ping(self):
        self.assertEqual(reply(b"*1\r\n$4\r\nPING\r\n"),
                         b"+PONG\r\n")

    def test_set(self):
        self.assertEqual(reply(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$4\r\nvalue\r\n"),
                         b"+OK\r\n")
        self.assertEqual(store[b"key"][b"value"],b"value")
        self.assertEqual(store[b"key"][b"expire"], datetime.max)

    def test_set_px(self):
        self.assertEqual(reply(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$4\r\nvalue\r\n$2\r\nPX\r\n$3\r\n100"),
                         b"+OK\r\n")
        self.assertEqual(store[b"key"][b"value"],b"value")
        self.assertEqual(store[b"key"][b"expire"], datetime.max)

    def test_bulk_string(self):
        self.assertEqual(bulk_string(b"ECHO"), b"$4\r\nECHO\r\n")

    def test_simple_string(self):
        self.assertEqual(parse(b"+ECHO\r\n"), "ECHO")

    def test_simple_int(self):
        self.assertEqual(parse(b":10\r\n"), 10)

    def test_simple_bulk(self):
        self.assertEqual(parse(b"$4\r\nECHO\r\n"), b"ECHO")

    def test_array1(self):
        self.assertEqual(parse(b"*2\r\n:3\r\n+ECHO\r\n"), [3, "ECHO"])

    def test_array(self):
        self.assertEqual(parse(b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"),
                         [b"ECHO", b"hey"])

if __name__ == '__main__':
    unittest.main()