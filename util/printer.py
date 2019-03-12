# -*- coding: utf-8 -*-

import sys


def progress(percent=0, title=None):
    text = '=' * (percent // 2) + '>' + ' ' * ((100 - percent - 1) // 2)
    sys.stdout.write('\r' + '[%s] ' % (title or '') + text + '[%s%%]' % (percent + 1))
    sys.stdout.flush()
