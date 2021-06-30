# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.

Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.

BK-BASE 蓝鲸基础平台 is licensed under the MIT License.

License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import unittest

from dataflow.batch.exceptions.comp_execptions import BatchTimeCompareError
from dataflow.batch.utils.time_util import BatchTimeTuple


class BatchTimeTupleTest(unittest.TestCase):
    def test_format_convert(self):
        tuple1 = BatchTimeTuple()
        tuple1.from_jobnavi_format("-3W")
        self.assertEqual(tuple1.month, 0)
        self.assertEqual(tuple1.week, -3)
        self.assertEqual(tuple1.day, 0)
        self.assertEqual(tuple1.hour, 0)

        tuple1 = BatchTimeTuple()
        tuple1.from_jobnavi_format("1M")
        self.assertEqual(tuple1.month, 1)
        self.assertEqual(tuple1.week, 0)
        self.assertEqual(tuple1.day, 0)
        self.assertEqual(tuple1.hour, 0)

        tuple1 = BatchTimeTuple()
        tuple1.from_jobnavi_format("2d")
        self.assertEqual(tuple1.month, 0)
        self.assertEqual(tuple1.week, 0)
        self.assertEqual(tuple1.day, 2)
        self.assertEqual(tuple1.hour, 0)

        tuple1 = BatchTimeTuple()
        tuple1.from_jobnavi_format("1M+2H")
        self.assertEqual(tuple1.month, 1)
        self.assertEqual(tuple1.week, 0)
        self.assertEqual(tuple1.day, 0)
        self.assertEqual(tuple1.hour, 2)

        tuple1 = BatchTimeTuple()
        tuple1.from_jobnavi_format("1M+1d-2H")
        self.assertEqual(tuple1.month, 1)
        self.assertEqual(tuple1.week, 0)
        self.assertEqual(tuple1.day, 1)
        self.assertEqual(tuple1.hour, -2)

    def test_equal(self):
        tuple1 = BatchTimeTuple(month=2, week=3, day=10, hour=20)
        tuple2 = BatchTimeTuple(month=2, week=1, day=23, hour=44)
        self.assertEqual(tuple1, tuple2)

    def test_not_equal(self):
        tuple1 = BatchTimeTuple(month=2, week=3, day=10, hour=20)
        tuple2 = BatchTimeTuple(month=2, week=1, day=5, hour=3)
        self.assertNotEqual(tuple1, tuple2)

    def test_greater(self):
        tuple1 = BatchTimeTuple(month=2, week=3, day=10, hour=20)
        tuple2 = BatchTimeTuple(month=2, week=1, day=5, hour=3)
        self.assertGreater(tuple1, tuple2)

    def test_less(self):
        tuple1 = BatchTimeTuple(month=2, week=1, day=10, hour=20)
        tuple2 = BatchTimeTuple(month=2, week=3, day=5, hour=3)
        self.assertLess(tuple1, tuple2)

    def test_greater_equal(self):
        tuple1 = BatchTimeTuple(month=2, week=3, day=10, hour=20)
        tuple2 = BatchTimeTuple(month=2, week=1, day=5, hour=3)
        self.assertGreaterEqual(tuple1, tuple2)

        tuple3 = BatchTimeTuple(month=2, week=3, day=10, hour=20)
        tuple4 = BatchTimeTuple(month=2, week=3, day=10, hour=20)
        self.assertGreaterEqual(tuple3, tuple4)

    def test_less_equal(self):
        tuple1 = BatchTimeTuple(month=2, week=1, day=10, hour=20)
        tuple2 = BatchTimeTuple(month=2, week=3, day=5, hour=3)
        self.assertLessEqual(tuple1, tuple2)

        tuple3 = BatchTimeTuple(month=2, week=3, day=10, hour=20)
        tuple4 = BatchTimeTuple(month=2, week=3, day=10, hour=20)
        self.assertLessEqual(tuple3, tuple4)

    def test_plus(self):
        tuple1 = BatchTimeTuple(month=2, week=1, day=10, hour=20)
        tuple2 = BatchTimeTuple(month=2, week=3, day=5, hour=3)

        tuple3 = tuple1 + tuple2
        tuple4 = BatchTimeTuple(month=4, week=4, day=15, hour=23)
        self.assertEqual(tuple3, tuple4)

    def test_minus(self):
        tuple1 = BatchTimeTuple(month=2, week=1, day=10, hour=7)
        tuple2 = BatchTimeTuple(month=2, week=3, day=5, hour=3)

        tuple3 = tuple2 - tuple1
        tuple4 = BatchTimeTuple(month=0, week=0, day=0, hour=212)
        self.assertEqual(tuple3, tuple4)

    def test_exception(self):
        tuple1 = BatchTimeTuple(month=0, week=0, day=58, hour=0)
        tuple2 = BatchTimeTuple(month=2, week=0, day=0, hour=0)
        with self.assertRaises(BatchTimeCompareError):
            tuple1 > tuple2

        with self.assertRaises(BatchTimeCompareError):
            tuple1 < tuple2


if __name__ == "__main__":
    unittest.main()
