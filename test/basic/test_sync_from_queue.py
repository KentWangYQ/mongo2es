import unittest

from module.elasticsearch.rts.sync_from_queue import SyncFromQueue

sfq = SyncFromQueue()


class SyncFromQueueTest(unittest.TestCase):
    def test_sync_from_queue(self):
        sfq.sync_from_queue()
