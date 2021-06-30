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
# from common.api import CCApi
from common.auth import check_perm, perm_check
from common.decorators import detail_route, list_route, params_valid
from common.django_utils import DataResponse, JsonResponse
from common.exceptions import DemoKVError
from common.local import get_request_app_code, get_request_username
from common.views import APIViewSet
from django.forms import model_to_dict
from rest_framework.response import Response

from .api import MetaApi
from .models import DemoModel
from .serializers import DemoSerializer


class SimpleViewSet(APIViewSet):
    """
    最简单的接口例子
    """

    def run_with_success(self, request):
        """
        @api {get | post | put | patch | delete} /demo/succeed/ 正常响应
        @apiDescription 正常执行完所有操作，返回必要数据 data，APIViewSet 会自动填充相应的字段

            Response('ok')

                ->

            Response({
                "message": "ok",
                "code": "1500200",
                "data": "ok",
                "result": true
            })
        """
        return Response({"bk_username": get_request_username(), "bk_app_code": get_request_app_code()})

    def run_with_exception(self, request):
        """
        @api {get} /demo/fail/  错误响应
        @apiDescription 处理流程出错，主动抛出异常，中断流程，APIViewSet 自动处理异常，要求
            异常需要继承与 common.exceptions.BaseAPIError
        """
        # raise DemoError()
        raise DemoKVError(message_kv={"param1": "aaa", "param2": "bbb"})

    def return_with_json(self, request):
        """
        @api {get} /demo/return_with_json/ 自定义返回json格式内容
        @apiDescription 自己组装返标准的返回格式，不推荐
        """
        return JsonResponse({"result": True, "data": [], "message": "xxx", "code": "1500200"})

    def return_with_data_response(self, request):
        """
        @api {get} /demo/return_with_data_response/ 定制化返回内容
        @apiDescription 支持部分字段重新定义返回，同时保留完整的响应结构
        """
        return DataResponse(result=True, message="self-message")

    def get_params(self, request):
        """
        @api {get} /demo/get_params/ 自定义返回json格式内容
        @apiDescription 自己组装返标准的返回格式，不推荐
        """
        return Response(request.query_params)

    def post_params(self, request):
        """
        @api {get} /demo/post_params/ 自定义返回json格式内容
        @apiDescription 自己组装返标准的返回格式，不推荐
        """
        return Response(request.data)


class ApiDemoViewSet(APIViewSet):
    def list(self, request):
        # 获取单个result_table信息
        res = MetaApi.result_tables.retrieve({"result_table_id": "table1"})
        # res.data         ==> 接口返回的data内容
        # res.message      ==> 接口返回的message
        # res.is_success() ==> 接口返回的result是否为True
        # res.code         ==> 接口返回的code

        # 获取result_table列表
        # GET /v3/meta/result_tables/?bk_biz_id=132
        res = MetaApi.result_tables.list({"bk_biz_id": 132}, timeout=10)
        # 可以在调用API的时候增加timeout(s)参数来覆盖默认的超时时间(60s)

        # 创建result_table
        # POST /v3/meta/result_tables/
        res = MetaApi.result_tables.create({"result_table_id": "table2", "description": "table2"})

        # 获取result_table的字段信息
        # GET /v3/meta/result_tables/table1/fields/
        res = MetaApi.result_tables.fields({"result_table_id": "table1"})

        # 获取result_table的存储信息
        # GET /v3/meta/result_tables/table1/storages/
        res = MetaApi.result_tables.storages({"result_table_id": "table1"})

        # 旧API（非restful风格）的调用
        # GET /trt/get_resutl_tables?result_table_id=table1
        res = MetaApi.get_result_table({"result_table_id": "table1"})

        # 二级资源的调用
        # GET /v3/demo/instances/11/sub_instances/12/
        res = MetaApi.sub_instances.retrieve({"instance_id": 11, "sub_instance_id": 12})

        # 非标准格式的API调用
        # GET http://i_am_a_nonstandard_api.com/aaa/bb
        res = MetaApi.nonstandard_api({"param1": 123}, raw=True)
        # 加了raw=True的参数后，将返回接口的完整返回
        # res ==> {
        #     'not_result': True,
        #     'not_message': 'xxxxxxx'
        # }

        # 获取业务信息，具体参数请参考cc文档
        # res = CCApi.get_app_by_id({"app_id": 132})

        return Response(res.data)

    def get_result_table(self, request):
        res = MetaApi.get_result_table({"result_table_id": "591_custom_model1"})
        return Response(res.data)


class SimpleFlowViewSet(APIViewSet):
    def get_flow_list(self, request):
        """
        @api {get} /demo/flows/ 获取Flow列表
        """
        return Response([])

    def get_flow_by_id(self, request, flow_id):
        """
        @api {get} /demo/flows/:flow_id/ 获取单个Flow详情
        """
        return Response({})


class ValidatorViewSet(APIViewSet):
    """
    参数校验例子样式，Class-based ViewSet 可以在实例函数中调用，也可以通过装饰器来使用

    结合DemoSerializer定义的结构，将得到结构如下的params:
    {
        "bk_biz_id": 122,
        "project_id": 2343,
        "name": "demo",
        "config": [{
            "aggregator": "sum",
            "numbers": [1, 2, 3],
            "detail": {}
        }, {
            "aggregator": "avg",
            "numbers": [4, 5, 6],
            "detail": {
                "any_params": "haha"
            }
        }]
    }
    """

    def get(self, request):
        """
        @api {get} /demo/validator/  自动调用参数校验方法
        """
        # 以实例函数调用时，返回值为校验成功时的参数集合
        params = self.params_valid(serializer=DemoSerializer)
        return Response(params)

    @params_valid(serializer=DemoSerializer)
    def post(self, request, params):
        """
        @api {post} /demo/validator/  通过装饰器申明参数校验的 Serializer
        """
        return Response("ok")


class InstanceViewset(APIViewSet):
    """
    对于资源 REST 操作逻辑统一放置在 APIViewSet 中提供接口
    """

    serializer_class = DemoSerializer

    # 对于URL中实例ID变量名进行重命名，默认为 pk
    lookup_field = "instance_id"

    def list(self, request):
        """
        @api {get} /demo/instances/ 获取实例列表
        """
        a = [i for i in range(0, 100000)]
        return Response(a)
        # return Response(list(DemoModel.objects.all().values()))

    def create(self, request):
        """
        @api {post} /demo/instances/ 新增实例
        """
        data = request.data
        obj = DemoModel.objects.create(
            field1=data["field1"],
            field2=data["field2"],
            field3=data["field3"],
        )
        return Response(model_to_dict(obj))

    def retrieve(self, request, instance_id):
        """
        @api {get} /demo/instances/:instance_id/ 获取单个实例详情
        """
        obj = DemoModel.objects.get(id=instance_id)
        return Response(model_to_dict(obj))

    def update(self, request, instance_id):
        """
        @api {put} /demo/instances/:instance_id/ 替换实例内容
        """
        obj = DemoModel.objects.get(id=instance_id)
        obj.field1 = request.data["field1"]
        obj.field2 = request.data["field2"]
        obj.field3 = request.data["field3"]
        obj.save()
        return Response(model_to_dict(obj))

    def partial_update(self, request, instance_id):
        """
        @api {patch} /demo/instances/:instance_id/ 更新实例部分内容
        """
        obj = DemoModel.objects.get(id=instance_id)
        obj.field1 = request.data["field1"]
        obj.save()
        return Response(model_to_dict(obj))

    def destroy(self, request, instance_id):
        """
        @api {delete} /demo/instances/:instance_id/ 删除实例
        """
        obj = DemoModel.objects.get(id=instance_id)
        obj.delete()
        return Response("ok")

    @list_route(methods=["get"], url_path="top")
    def top(self, request):
        """
        @api {post} /demo/instances/top/ 获取置顶的实例列表
        """
        return Response("top")

    @detail_route(methods=["post"], url_path="do_something")
    def do_something(self, request, instance_id):
        """
        @api {post} /demo/instances/:instance_id/do_action/
        """
        return Response("do_something {}".format(instance_id))

    @perm_check(action_id="result_table.query_data")
    @detail_route(methods=["get"], url_path="check_perm_detail")
    def check_perm_detail(self, request, instance_id):
        """
        @api {get} /demo/instances/:instance_id/check_perm/ 校验权限样例
        @apiDescription 样例演示
        """
        # 权限不足，会自动抛出异常
        # 未传递 app_code、username，会从环境变量中获取
        check_perm("result_table.query_data", "1_xsx")
        return Response("ok")

    @perm_check(action_id="result_table.create", detail=False)
    @list_route(methods=["get"], url_path="check_perm_list")
    def check_perm_list(self, request, instances_id):
        """
        @api {get} /demo/instances/:instance_id/check_perm/ 校验权限样例
        @apiDescription 样例演示
        """
        # 权限不足，会自动抛出异常
        # 若是无对象的操作操作校验 object_id 传 None 或者不传
        check_perm("result_table.create", None)

        return Response("ok")


class UserInstanceViewset(APIViewSet):
    lookup_field = "instance_id"

    def list(self, request, username):
        """
        @api {get} /demo/instances/:username/ 获取用户私有的实例列表
        """
        return Response("list {}.instances".format(username))


class SubInstanceViewset(APIViewSet):
    lookup_field = "sub_instance_id"

    def list(self, request, instance_id):
        """
        @api {get} /demo/instances/:instance_id/sub_instances/ 获取子实例列表
        """
        return Response("list {}.sub_instances".format(instance_id))


class AuthTestViewset(APIViewSet):
    """
    这个 ViewSet 仅用于测试 common/auth 文件夹内的代码，后续会移除
    """

    def test_identity(self, request):
        print(11)
        print(request.META.get("HTTP_X_BKAPI_AUTHORIZATION", ""))
        print(22)
        from common.auth import check_perm
        from common.auth.middlewares import get_identity

        identity = get_identity()

        print(identity)

        check_perm("result_table.query_data", "591_jere_node2", raise_exception=True)

        return Response("ok")

    def test_redirect_other_api(self, request):
        import requests

        session = requests.session()
        session.headers.update({"x-bkapi-authorization": '{"bkdata":"aaa"}'})
        response = session.request(method="GET", url="http://127.0.0.1:8000/v3/demo/auth/test_identity/", params={})
        return Response(response.data)
