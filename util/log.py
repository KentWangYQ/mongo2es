# -*- coding: utf-8 -*-

import sys
import logging
from config import FLASK_DEBUG

# 使用一个名字为mongo2es的logger
logger = logging.getLogger('mongo2es')

# 创建一个输出日志到控制台的StreamHandler
hdr = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
hdr.setFormatter(formatter)

# 给logger添加上handler
logger.addHandler(hdr)

if FLASK_DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
