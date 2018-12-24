# -*- coding: utf-8 -*-

# Note:
# index和type必须为全小写

es_errors = {
    'index': 'errors',
    'type': {
        'exception': 'exception'
    },
    'settings': {
        'number_of_shards': 3,
        'number_of_replicas': 0
    },
    'mappings': {}
}

car_change_plans = {
    'index': 'carchangeplans',
    'type': {
        'used_car': 'usedcar',
        'car_change_plan': 'carchangeplan',
        'car_offer': 'caroffer',
        'used_car_offer': 'usedcaroffer',
        'order': 'order',
        'wx_order': 'wxorder',
        'pos_order': 'posorder'
    },
    'routing': 'carchangeplans',
    'mappings': {
        'usedcar': {},
        'carchangeplan': {
            '_parent': {
                'type': 'usedcar'
            }
        },
        'usedcaroffer': {
            '_parent': {
                'type': 'usedcar'
            }
        },
        'caroffer': {
            '_parent': {
                'type': 'carchangeplan'
            }
        },
        'order': {
            '_parent': {
                'type': 'carchangeplan'
            }
        },
        'wxorder': {
            '_parent': {
                'type': 'order'
            }
        },
        'posorder': {
            '_parent': {
                'type': 'order'
            }
        }
    },
    'settings': {
        'number_of_shards': 3,
        'number_of_replicas': 0
    }
}

tracks = {
    'index': 'tracks',
    'type': {
        'impression_track': 'impressiontrack',
        'act_share_detail': 'actsharedetail'
    },
    'routing': 'tracks',
    'mappings': {},
    'settings': {
        'number_of_shards': 3,
        'number_of_replicas': 0
    }
}
