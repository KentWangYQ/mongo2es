# -*- coding: utf-8 -*-

# 1定义
# 2消息（1字头）
# ▪ 100 Continue
# ▪ 101 Switching Protocols
# ▪ 102 Processing
# 3成功（2字头）
# ▪ 200 OK
# ▪ 201 Created
# ▪ 202 Accepted
# ▪ 203 Non-Authoritative Information
# ▪ 204 No Content
# ▪ 205 Reset Content
# ▪ 206 Partial Content
# ▪ 207 Multi-Status
# 4重定向（3字头）
# ▪ 300 Multiple Choices
# ▪ 301 Moved Permanently
# ▪ 302 Move temporarily
# ▪ 303 See Other
# ▪ 304 Not Modified
# ▪ 305 Use Proxy
# ▪ 306 Switch Proxy
# ▪ 307 Temporary Redirect
# 5请求错误（4字头）
# ▪ 400 Bad Request
# ▪ 401 Unauthorized
# ▪ 402 Payment Required
# ▪ 403 Forbidden
# ▪ 404 Not Found
# ▪ 405 Method Not Allowed
# ▪ 406 Not Acceptable
# ▪ 407 Proxy Authentication Required
# ▪ 408 Request Timeout
# ▪ 409 Conflict
# ▪ 410 Gone
# ▪ 411 Length Required
# ▪ 412 Precondition Failed
# ▪ 413 Request Entity Too Large
# ▪ 414 Request-URI Too Long
# ▪ 415 Unsupported Media Type
# ▪ 416 Requested Range Not Satisfiable
# ▪ 417 Expectation Failed
# ▪ 421 There are too many connections from your internet address
# ▪ 422 Unprocessable Entity
# ▪ 423 Locked
# ▪ 424 Failed Dependency
# ▪ 425 Unordered Collection
# ▪ 426 Upgrade Required
# ▪ 449 Retry With
# 6服务器错误（5字头）
# ▪ 500 Internal Server Error
# ▪ 501 Not Implemented
# ▪ 502 Bad Gateway
# ▪ 503 Service Unavailable
# ▪ 504 Gateway Timeout
# ▪ 505 HTTP Version Not Supported
# ▪ 506 Variant Also Negotiates
# ▪ 507 Insufficient Storage
# ▪ 509 Bandwidth Limit Exceeded
# ▪ 510 Not Extended
# ▪ 600 Unparseable Response Headers
from common.rst import Rst


class StatusCode(Exception):
    @property
    def code(self):
        return self.__code

    @property
    def message(self):
        return self.__message

    def __init__(self, code, message):
        self.__code = code
        self.__message = message


error = {

    # 请求失败，Request is invalid
    'BadRequest': {
        'status': 400,
        'message': {
            'statusCode': 400001,
            'message': '请求失败'
        }
    },

    # 请求失败，Missing Parameter
    'MissingParameter': {
        'status': 400,
        'message': {
            'statusCode': 400002,
            'message': '缺少参数'
        }
    },

    # ES 请求失败，Out of result window
    'ES_OUTOFRESULTWINDOW': {
        'status': 400,
        'message': {
            'statusCode': 400100,
            'message': '(ES)分页数据总数超过10000条,请使用游标方法'
        }
    },

    # 请求的token是无效的,Token is invalid
    'TokenInvalid': {
        'status': 401,
        'message': {
            'statusCode': 401001,
            'message': '请求的token是无效的'
        }
    },

    # 请求的签名是无效的
    'SignVerificationInvalid': {
        'status': 401,
        'message': {
            'statusCode': 401002,
            'message': '请求的签名无效'
        }
    },

    # 请求非法, Request is forbidden
    'Forbidden': {
        'status': 403,
        'message': {
            'statusCode': 403001,
            'message': '请求非法'
        }
    },

    # 用户已经存在,User Already Exist
    'UserAlreadyExist': {
        'status': 403,
        'message': {
            'statusCode': 403002,
            'message': '用户已经存在'
        }
    },

    # 用户没有找到,Can not find the users
    'UserNotFound': {
        'status': 403,
        'message': {
            'statusCode': 403003,
            'message': '用户没有找到'
        }
    },

    'UserProfileNotFound': {
        'status': 403,
        'message': {
            'statusCode': 403008,
            'message': '用户profile数据没有找到'
        }
    },

    # 密码不正确,The password is incorrect
    'PasswordIncorrect': {
        'status': 403,
        'message': {
            'statusCode': 403004,
            'message': '密码不正确'
        }
    },

    # 达到最大重试次数,Reached the max attemps
    'MaxAttemps': {
        'status': 403,
        'message': {
            'statusCode': 403005,
            'message': '达到最大重试次数'
        }
    },

    'UserAlreadyBinding': {
        'status': 403,
        'message': {
            'statusCode': 403006,
            'message': '用户已经被绑定'
        }
    },

    'InvalidVerifyCode': {
        'status': 403,
        'message': {
            'statusCode': 403007,
            'message': '验证码不正确'
        }
    },

    'UnAuthorizedRole': {
        'status': 403,
        'message': {
            'statusCode': 403008,
            'message': '账户未授权'
        }
    },
    # 未找到对应api,Request not found'
    'NotFound': {
        'status': 404,
        'message': {
            'statusCode': 404001,
            'message': '未找到对应api'
        }
    },

    'ThirdPartyServerFailed': {
        'status': 405,
        'message': {
            'statusCode': 405001,
            'message': '三方依赖服务不可用'
        }
    },

    # 违反唯一约束,Unique constraint failed for this request
    'Conflict': {
        'status': 409,
        'message': {
            'statusCode': 409001,
            'message': '违反唯一约束'
        }
    },

    # 不支持的请求,Unsupported protocol version
    'Unsupported': {
        'status': 410,
        'message': {
            'statusCode': 410001,
            'message': '不支持的请求'
        }
    },

    # 超出内容限制,Over capacity
    'OverCapacity': {
        'status': 412,
        'message': {
            'statusCode': 412001,
            'message': '超出内容限制'
        }
    },

    # 请求体过长,Request entity is too large
    'TooLarge': {
        'status': 413,
        'message': {
            'statusCode': 413001,
            'message': '请求体过长'
        }
    },

    # 验证码超过长度限制
    'VerifyCodeTooLarge': {
        'status': 413,
        'message': {
            'statusCode': 413002,
            'message': '验证码超过长度限制'
        }
    },

    # 账户已被封禁
    'LOCKED': {
        'status': 423,
        'message': {
            'statusCode': 423001,
            'message': "账户已被封禁"
        }
    },

    # 依赖检查失败,This request failed because its dependencies is missing or invalid
    'DependencyFailure': {
        'status': 424,
        'message': {
            'statusCode': 424001,
            'message': '依赖检查失败'
        }
    },

    # 请使用https安全通道请求,This request require HTTPS to ensure security
    'Insecure': {
        'status': 426,
        'message': {
            'statusCode': 426001,
            'message': '请使用https安全通道请求'
        }
    },

    # 未知服务器内部错误,ServerInternalError
    'ServerInternalError': {
        'status': 500,
        'message': {
            'statusCode': 500001,
            'message': '服务器开小差了'
        }
    },

    # 未知nginx内部错误,Upstream service error
    'UpstreamError': {
        'status': 502,
        'message': {
            'statusCode': 502001,
            'message': '未知nginx内部错误'
        }
    },

    # 未知服务器内部错误,ServerInternalError
    'ServerMaintain': {
        'status': 503,
        'message': {
            'statusCode': 503001,
            'message': '服务器正在维护'
        }
    }
}


def __parse(key):
    return StatusCode(code=error[key].get('status'),
                      message=Rst(
                          error=True,
                          data=None,
                          status_code=error[key]['message']['statusCode'],
                          message=error[key]['message']['message']))


BadRequest = __parse('BadRequest')
MissingParameter = __parse('MissingParameter')

ES_OUTOFRESULTWINDOW = __parse('ES_OUTOFRESULTWINDOW')

TokenInvalid = __parse('TokenInvalid')
SignVerificationInvalid = __parse('SignVerificationInvalid')
Forbidden = __parse('Forbidden')
UserAlreadyExist = __parse('UserAlreadyExist')
UserNotFound = __parse('UserNotFound')
UserProfileNotFound = __parse('UserProfileNotFound')
PasswordIncorrect = __parse('PasswordIncorrect')
MaxAttemps = __parse('MaxAttemps')
UserAlreadyBinding = __parse('UserAlreadyBinding')
InvalidVerifyCode = __parse('InvalidVerifyCode')
UnAuthorizedRole = __parse('UnAuthorizedRole')
NotFound = __parse('NotFound')
ThirdPartyServerFailed = __parse('ThirdPartyServerFailed')
Conflict = __parse('Conflict')
Unsupported = __parse('Unsupported')
OverCapacity = __parse('OverCapacity')
TooLarge = __parse('TooLarge')
VerifyCodeTooLarge = __parse('VerifyCodeTooLarge')
LOCKED = __parse('LOCKED')
DependencyFailure = __parse('DependencyFailure')
Insecure = __parse('Insecure')
ServerInternalError = __parse('ServerInternalError')
UpstreamError = __parse('UpstreamError')
ServerMaintain = __parse('ServerMaintain')
