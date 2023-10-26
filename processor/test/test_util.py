import unittest

import processor.util as util


class TestUtil(unittest.TestCase):
    def test_chunk(self):
        numbers = range(0, 1001)
        chunks = [c for c in util.chunks(numbers, 10)]
        for chunk in chunks[0:99]:
            assert len(chunk) == 10
        assert len(chunks[100]) == 1
        chunks = [c for c in util.chunks(numbers, 100)]
        assert len(chunks) == 11
        for chunk in chunks[0:9]:
            assert len(chunk) == 100
        assert len(chunks[10]) == 1
