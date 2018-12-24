import unittest
from common.constant import ErrorStatus
from module.elasticsearch.query import es_errors


class EsErrorsTest(unittest.TestCase):
    def test_exception_search(self):
        result = es_errors.exception.search(status='open', error_code=400, error_type='mapper_parsing_exception',
                                            start='2018-05-16 00:00:00', end='2018-05-20 01:02:03')

        print(result)

    def test_exception_update(self):
        result = es_errors.exception.change_status(error_id='AWNylzmBPWHSSx8SU-lG', status=ErrorStatus.resolved)
        print(result)

    def test_exception_delete(self):
        result = es_errors.exception.delete_error(error_id='AWNymsaSPWHSSx8SU-lM')
        print(result)
