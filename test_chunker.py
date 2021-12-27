import unittest

from chunker import chunks

class Test(unittest.TestCase):
    def test(self):
        actual = []
        for chunk in chunks(range(5), 2):
            actual.append(list(chunk))
        expected = [[0,1], [2,3], [4]]

        
