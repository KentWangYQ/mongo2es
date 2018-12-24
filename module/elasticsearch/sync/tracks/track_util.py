# -*- coding: utf-8 -*-


import re
import urlparse

from common.log import logger

import sys

reload(sys)
sys.setdefaultencoding("utf-8")


def url_split(url):
    obj = {}
    if url:
        r = urlparse.urlparse(url.lower())
        obj = {
            'scheme': r.scheme,
            'hostname': r.hostname,
            'path': r.path,
            'params': {}
        }
        
        if r.query:
            for q in re.split('&', r.query):
                if q:
                    #处理url中的中文
                    try:
                        q = targetDecode(q)#urlparse.unquote(q).decode('utf-8')
                    except Exception as e:
                        logger.debug(q)
                        logger.exception(e.message)
                    
                    pa = re.split('=', q)
                    if pa[0]:
                        obj['params'][pa[0].strip()] = pa[1].strip() if len(pa) > 1 else ''

    return obj

def targetDecode(target, encoding='utf-8', errors='strict'):
    return urlparse.unquote(
        target.encode(encoding, errors)
    ).decode(encoding, errors)