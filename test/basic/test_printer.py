# -*- coding: utf-8 -*-

import unittest
import time
from util import printer


class PrinterTest(unittest.TestCase):
    def test_progress(self):
        for i in range(100):
            printer.progress(percent=i, title='progress printer test')
            time.sleep(0.05)
