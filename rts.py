# -*- coding: utf-8 -*-
import time
from multiprocessing import Pool

from config import FLASK_ENV
from common.log import logger
from common.sentry import SentryClient

from module.elasticsearch.rts import real_time_sync, sync_from_queue

sentry_client = SentryClient(ignore_exceptions=['KeyboardInterrupt'])


def sync_from_queue_start():
    sync_from_queue.sfq.start()


if __name__ == "__main__":

    try:
        pool = Pool(processes=10)

        workers = [
            sync_from_queue_start,  # 从队列同步
        ]

        for worker in workers:
            pool.apply_async(worker)

        # 实时同步, mongo client 无法在fork进程中运行，作为主进程运行
        real_time_sync.rts.start()
        # time.sleep(10000000)

    except Exception as e:
        if FLASK_ENV != 'development':
            sentry_client.error(logger.exception(e))
            logger.exception(e)
        else:
            logger.exception(e)
