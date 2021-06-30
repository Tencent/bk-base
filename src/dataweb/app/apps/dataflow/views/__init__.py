# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
import json

from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from apps.api import PASS_THROUGH_MODULE_MAP
from apps.api.base import PassThroughAPI
from apps.common.log import logger
from apps.exceptions import DataError, PermissionError
from apps.utils.image import produce_watermark


def home(request):
    return render(request, 'index.html', {})


@csrf_exempt
def pass_through(request, module, sub_url):
    """
    直接透传接口
    """
    if request.method == "GET":
        api_params = dict(request.GET)
    else:
        try:
            api_params = json.loads(request.body)
        except ValueError:
            api_params = dict(request.POST)
            for key, value in list(request.FILES.items()):
                api_params[key] = value

    module_api = PASS_THROUGH_MODULE_MAP.get(module)
    if not module_api:
        raise PermissionError("%s模块暂不支持透传" % module)
    api = PassThroughAPI(module_api.MODULE, request.method, module_api.URL_PREFIX, sub_url)
    try:
        data = api(api_params)
    except DataError as e:
        return JsonResponse(
            {
                "data": None,
                "message": e.message,
                "code": e.code,
                "errors": getattr(e, "errors", {}),
                "result": False,
            }
        )
    except Exception as e:
        logger.exception("Fail to pass through: {}".format(e))
        return JsonResponse(
            {
                "data": None,
                "message": str(e),
                "code": "1520500",
                "errors": None,
                "result": False,
            }
        )
    else:
        return JsonResponse({"data": data, "message": "", "code": "00", "result": True})


def signature(request):
    """
    @api {get} /signature.png 获取用户的水印图片
    @apiName signature
    @apiGroup Others
    """
    username = request.user.username

    image = produce_watermark(username)
    # serialize to HTTP response
    response = HttpResponse()
    response["Content-Type"] = "image/png"
    image.save(response, "PNG")
    return response
