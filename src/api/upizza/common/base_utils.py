# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import datetime
import decimal
import json
import uuid

from common.exceptions import ValidationError
from django.conf import settings
from django.db import router
from django.utils.translation import ugettext as _
from rest_framework import exceptions, serializers

from .auth.exceptions import BaseAuthError
from .django_utils import JsonResponse
from .exceptions import BaseAPIError
from .log import logger


def generate_request_id():
    """
    create randmon request_id
    """
    return uuid.uuid4().hex


def make_resp_dict(result=False, code=None, data=None, message="", errors=None):
    """
    根据给定的字段来生成一个标准的HttpResponse数据
    """
    if code is None:
        code = "00" if result else "50000"

    response = {"result": result, "code": code, "data": data, "message": message, "errors": errors}
    return response


def format_resp_dict(resp_data):
    """
    根据给定的数据生成一个标准的HttpResponse数据
    """
    resp_data.setdefault("result", False)
    resp_data.setdefault("data", None)
    resp_data.setdefault("message", "")
    resp_data.setdefault("code", "00" if resp_data["result"] else "50000")
    resp_data.setdefault("errors", None)
    return resp_data


def custom_exception_handler(exc, context):
    """
    自定义错误处理方式
    """
    if isinstance(exc, exceptions.APIException):  # 默认错误统一处理
        logger.exception(f"DRFError: {exc}")
        return JsonResponse(make_resp_dict(code="1500{}".format(exc.status_code), message=exc.detail), is_log_resp=True)
    elif isinstance(exc, BaseAPIError):
        logger.exception(f"BaseAPIError: {exc}")
        return JsonResponse(
            {"result": False, "data": None, "message": exc.message, "code": exc.code, "errors": exc.errors},
            is_log_resp=True,
        )
    elif isinstance(exc, BaseAuthError):
        logger.warning(f"BaseAuthError: {exc}")
        return JsonResponse({"result": False, "data": None, "message": exc.message, "code": exc.code, "errors": None})
    else:
        message = str(exc)
        logger.exception(f"Not expected exception: {message}, {context}")
        return JsonResponse(make_resp_dict(code="1500500", message=message), is_log_resp=True)


def get_db_settings_by_model(model):
    concrete_model = model._meta.concrete_model
    db_alias = router.db_for_write(concrete_model)
    return settings.DATABASES.get(db_alias, None)


def model_to_dict(instance, fields=None, exclude=None):
    """Return django model Dict, Override django model_to_dict: <foreignkey use column as key>"""
    opts = instance._meta
    data = {}
    from itertools import chain

    for f in chain(opts.concrete_fields, opts.private_fields, opts.many_to_many):
        key = f.name
        if fields and f.name not in fields:
            continue
        if exclude and f.name in exclude:
            continue
        if f.get_internal_type() == "ForeignKey":
            key = f.column
        data[key] = f.value_from_object(instance)
    return data


class CustomJSONEncoder(json.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time and decimal types.
    And process the smart place name object
    """

    DATE_FORMAT = "%Y-%m-%d"
    TIME_FORMAT = "%H:%M:%S"

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime("{} {}".format(self.DATE_FORMAT, self.TIME_FORMAT))
        elif isinstance(o, datetime.date):
            return o.strftime(self.DATE_FORMAT)
        elif isinstance(o, datetime.time):
            return o.strftime(self.TIME_FORMAT)
        elif isinstance(o, decimal.Decimal):
            return float(o)
        elif isinstance(o, set):
            return list(o)
        else:
            try:
                return super(CustomJSONEncoder, self).default(o)
            except Exception:
                return "<Type: %s>" % type(o).__name__


def jsonize(d):
    return json.dumps(d, cls=CustomJSONEncoder, ensure_ascii=True)


def str_bool(value):
    """
    Convert string to boolean.

        >>> str_bool("0")
        False
        >>> str_bool("1")
        True
        >>> str_bool("true")
        True
        >>> str_bool("false")
        False
    """
    if isinstance(value, str):
        value = value.strip()
        if value.lower() in ("0", "false"):
            return False
    return bool(value)


def request_fetch(request, key, default="", get_first=False):
    if get_first:
        fetch_arr = [request.GET, request.POST, request.data]
    else:
        fetch_arr = [request.POST, request.data, request.GET]
    for arr in fetch_arr:
        if key in arr:
            return arr.get(key, default)
    return default


def safe_int(num_str, dft_value=0):
    result = dft_value
    try:
        result = int(num_str)
    except ValueError:
        pass
    return result


def format_serializer_errors(errors, fields, params, prefix="  "):
    # 若只返回其中一条校验错误的信息，则只需要把注释的三个return打开即可
    message = _("参数校验失败:{wrap}").format(wrap="\n") if prefix == "  " else "\n"
    for key, field_errors in list(errors.items()):
        sub_message = ""
        label = key
        if key not in fields:
            sub_message = json.dumps(field_errors, ensure_ascii=False)
        else:
            field = fields[key]
            label = field.label or field.field_name
            if (
                hasattr(field, "child")
                and isinstance(field_errors, list)
                and len(field_errors) > 0
                and not isinstance(field_errors[0], str)
            ):
                for index, sub_errors in enumerate(field_errors):
                    if sub_errors:
                        sub_format = format_serializer_errors(
                            sub_errors, field.child.fields, params, prefix=prefix + "    "
                        )
                        # return sub_format
                        sub_message += _("{wrap}{prefix}第{index}项:").format(
                            wrap="\n",
                            prefix=prefix + "  ",
                            index=index + 1,
                        )
                        sub_message += sub_format
            else:
                if isinstance(field_errors, dict):
                    if hasattr(field, "child"):
                        sub_foramt = format_serializer_errors(
                            field_errors, field.child.fields, params, prefix=prefix + "  "
                        )
                    else:
                        sub_foramt = format_serializer_errors(field_errors, field.fields, params, prefix=prefix + "  ")
                    # return sub_foramt
                    sub_message += sub_foramt
                elif isinstance(field_errors, list):
                    for index, error in enumerate(field_errors):
                        field_errors[index] = field_errors[index].format(**{key: params.get(key, "")})
                        # return field_errors[index]
                        sub_message += "{index}.{error}".format(index=index + 1, error=field_errors[index])
                    sub_message += "\n"
        # 对使用 Validate() 时 label == 'non_field_errors' 的特殊情况做处理
        if label == "non_field_errors":
            message += "{prefix} {message}".format(prefix=prefix, message=sub_message)
        else:
            message += "{prefix}{label}: {message}".format(prefix=prefix, label=label, message=sub_message)
    return message


def custom_params_valid(serializer, params, many=False):
    _serializer = serializer(data=params, many=many)
    try:
        _serializer.is_valid(raise_exception=True)
    except serializers.ValidationError:
        try:
            message = format_serializer_errors(_serializer.errors, _serializer.fields, params)
        except Exception as e:
            if isinstance(e.message, str):
                message = e.message
            else:
                message = _("参数校验失败，详情请查看返回的errors")
        raise ValidationError(message=message, errors=_serializer.errors)
    if many:
        return list(_serializer.validated_data)
    else:
        return dict(_serializer.validated_data)


def underline_to_camel(underline_format):
    """
    下划线命名格式驼峰命名格式
    """
    camel_format = ""
    if isinstance(underline_format, str):
        for _s_ in underline_format.split("_"):
            camel_format += _s_.capitalize()
    return camel_format


def generate_modelviewset_apidoc(modelviewset, url_prefix, alias, version):
    model_name = modelviewset.model._meta.db_table
    primary_key = modelviewset.model._meta.pk.name
    group_name = underline_to_camel(model_name)
    return """
        \"\"\"
        @api {{get}} {url_prefix}/{model_name}s/ 获取{alias}列表
        @apiVersion {version}
        @apiGroup {group_name}
        @apiName get_{model_name}_list

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {{
                "data": [
                    {{

                    }}
                ],
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }}
        \"\"\"
        \"\"\"
        @api {{get}} {url_prefix}/{model_name}s/:{primary_key}/ 获取{alias}详情
        @apiVersion {version}
        @apiGroup {group_name}
        @apiName get_{model_name}_detail

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {{
                "data": {{

                }},
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }}
        \"\"\"
        \"\"\"
        @api {{post}} {url_prefix}/{model_name}s/ 创建{alias}配置
        @apiVersion {version}
        @apiGroup {group_name}
        @apiName create_{model_name}

        @apiParamExample {{json}} 参数样例:
            {{

            }}

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {{
                "data": {{

                }},
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }}
        \"\"\"
        \"\"\"
        @api {{put}} {url_prefix}/{model_name}s/:{primary_key}/ 修改{alias}配置
        @apiVersion {version}
        @apiGroup {group_name}
        @apiName update_{model_name}

        @apiParamExample {{json}} 参数样例:
            {{

            }}

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {{
                "data": {{

                }},
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }}
        \"\"\"
        \"\"\"
        @api {{patch}} {url_prefix}/{model_name}s/:{primary_key}/ 修改部分{alias}配置
        @apiVersion {version}
        @apiGroup {group_name}
        @apiName partial_update_{model_name}

        @apiParamExample {{json}} 参数样例:
            {{

            }}

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {{
                "data": {{

                }},
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }}
        \"\"\"
        \"\"\"
        @api {{delete}} {url_prefix}/{model_name}s/:{primary_key}/ 删除{alias}配置
        @apiVersion {version}
        @apiGroup {group_name}
        @apiName delete_{model_name}

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {{
                "data": {{

                }},
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }}
        \"\"\"
    """.format(
        url_prefix=url_prefix,
        model_name=model_name,
        group_name=group_name,
        version=version,
        alias=alias,
        primary_key=primary_key,
    )


def extract_parmas(request, key=None, default=None):
    """
    根据请求类型，提取请求参数
    @param request
    @param key string 需提取的键
    @param default 默认返回值
    """
    data = {}

    if request.method == "GET":
        data = request.GET
    # 请求方法 POST、PUT、PATCH、DELETE，从 body 中获取参数内容
    else:
        try:
            data = json.loads(request.body)
        except ValueError:
            pass

    if key is None:
        return data or default
    else:
        return data.get(key, default)
