# -*- coding: utf-8 -*-

import unittest
from full_sync import Sync

sync = Sync()


class FullSyncTest(unittest.TestCase):
    def test_full_index(self):
        sync.index_all()
