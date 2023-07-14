import string
import unittest
import io

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

def parse(bytes: bytes):
    """
    >>> parse(b'*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n')
    [b"ECHO",b"hey"]
    """
    return parseStream(io.BytesIO(bytes))

class TestFunc(unittest.TestCase):
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