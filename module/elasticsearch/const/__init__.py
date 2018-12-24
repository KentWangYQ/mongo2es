# -*- coding: utf-8 -*-
__all__ = ['index', 'base', 'structure']

from config import FLASK_ENV
import index

rts_key = 'ES_SYNC'
rts_reset_key = 'ES_SYNC_RESET'

__prefix = {
    'development': '',
    'test': '',
    'staging': '',
    'production': ''
}

__suffix = {
    'development': '_dev',
    'test': '_test',
    'staging': '_staging',
    'production': ''
}


def __get_es_info(env):
    info = index
    pfx = (__prefix[env] or '')
    sfx = (__suffix[env] or '')
    for k, p in vars(info).items():
        if not k.startswith('__'):
            if p.get('index'):
                p['index'] = '{0}{1}{2}'.format(pfx, p['index'], sfx)

    return info


structure = __get_es_info(FLASK_ENV)
