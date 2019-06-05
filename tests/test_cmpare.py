# encoding:utf-8
from unittest import TestCase

from tableseries.compare import DateCompare


class TestCompareUnitTest(TestCase):
    """
    """

    def test_eq(self):
        # equal
        a = DateCompare(2011, 2)
        b = DateCompare(2011, 2)
        self.assertTrue(a == b)
        a1 = DateCompare(2011, 2, 4)
        b1 = DateCompare(2011, 2, 5)
        self.assertFalse(a1 == b1)
        a2 = DateCompare(2011, 2)
        b2 = DateCompare(2011, 2, 3)
        self.assertFalse(a2 == b2)

    def test_gt(self):
        # great than >
        a = DateCompare(2011, 2)
        b = DateCompare(2011, 2)
        self.assertFalse(a > b)
        a1 = DateCompare(2011, 1)
        b1 = DateCompare(2010, 1)
        self.assertTrue(a1 > b1)
        a2 = DateCompare(2011, 2)
        b2 = DateCompare(2011, 3)
        self.assertFalse(a2 > b2)

    def test_ge(self):
        a = DateCompare(2011, 2)
        b = DateCompare(2011, 2)
        self.assertTrue(a >= b)
        a1 = DateCompare(2011, 1)
        b1 = DateCompare(2010, 1)
        self.assertTrue(a1 >= b1)
        a2 = DateCompare(2011, 2)
        b2 = DateCompare(2011, 3)
        self.assertFalse(a2 >= b2)

    def test_lt(self):
        a = DateCompare(2011, 2)
        b = DateCompare(2011, 2)
        self.assertFalse(a < b)
        a1 = DateCompare(2011, 1)
        b1 = DateCompare(2010, 1)
        self.assertTrue(b1 < a1)
        a2 = DateCompare(2011, 2)
        b2 = DateCompare(2011, 3)
        self.assertTrue(a2 < b2)

    def test_le(self):
        a = DateCompare(2011, 2)
        b = DateCompare(2011, 2)
        self.assertTrue(a <= b)
        a1 = DateCompare(2011, 1)
        b1 = DateCompare(2010, 1)
        self.assertTrue(b1 <= a1)
        a2 = DateCompare(2011, 2)
        b2 = DateCompare(2011, 3)
        self.assertTrue(a2 <= b2)
        a3 = DateCompare(2012, 2, 4)
        a4 = DateCompare(2013, 2)
        self.assertTrue(a3 <= a4)
