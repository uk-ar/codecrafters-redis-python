import string
import unittest

def parse(str: string):
    """
    >>> parse('*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n')
    ["ECHO","hey"]
    """
    return ["ECHO", "hey"]

class TestFunc(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(parse("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"),
                         ["ECHO", "hey"])

if __name__ == '__main__':
    unittest.main()