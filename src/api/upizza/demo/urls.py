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
from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter

from . import views

router = DefaultRouter(trailing_slash=True)

# 一级资源设计
# /demo/instances/ 其后的 REST-URLs 会根据 InstanceViewset 自动生成
# URL 方式推荐使用 DefaultRouter 的配置方式
router.register(r"instances", views.InstanceViewset, basename="instance")

# 一级资源设计（带受限范围）
router.register(r"instances/(?P<username>\w+)", views.UserInstanceViewset, basename="user_instance")

# 二级资源设计
router.register(r"instances/(?P<instance_id>\d+)/sub_instances", views.SubInstanceViewset, basename="sub_instance")

# 这里必须设置base_name，通过router注册rest路由时，实际上会生成多个路由
# 而base_name是用来给这多个路由命名的，这个命名的作用是用来生成url的，所以暂时用不到
# 比如上面的路由会生成如下url:
#    url(r'^instances/', name='instance-list')
#    url(r'^instances/(?P<instance_id>\d+)/', name='instance-detail'),
#    ...
# base_name就是这些路由的name参数的前缀


urlpatterns = [
    url(r"^", include(router.urls)),
    url(
        r"^succeed/$",
        views.SimpleViewSet.as_view(
            {
                "get": "run_with_success",
                "post": "run_with_success",
                "put": "run_with_success",
                "patch": "run_with_success",
                "delete": "run_with_success",
            }
        ),
    ),
    url(r"^fail/$", views.SimpleViewSet.as_view({"get": "run_with_exception"})),
    url(r"^return_with_json/$", views.SimpleViewSet.as_view({"get": "return_with_json"})),
    url(r"^return_with_data_response/$", views.SimpleViewSet.as_view({"get": "return_with_data_response"})),
    url(r"^get_params/$", views.SimpleViewSet.as_view({"get": "get_params"})),
    url(r"^post_params/$", views.SimpleViewSet.as_view({"post": "post_params"})),
    # 自行注册REST接口，URL的正则需要自己定义（不推荐）
    url(r"^flows/?$", views.SimpleFlowViewSet.as_view({"get": "get_flow_list"})),
    url(r"^flows/(?P<flow_id>\d+)/$", views.SimpleFlowViewSet.as_view({"get": "get_flow_by_id"})),
    url(r"^validator/$", views.ValidatorViewSet.as_view({"get": "get", "post": "post"})),
    url(r"^apidemo/get_result_table/$", views.ApiDemoViewSet.as_view({"get": "get_result_table"})),
    # 测试 AuthSDK 代码
    url(r"^auth/test_identity/$", views.AuthTestViewset.as_view({"get": "test_identity"})),
    url(r"^auth/test_redirect_other_api/$", views.AuthTestViewset.as_view({"get": "test_redirect_other_api"})),
]
