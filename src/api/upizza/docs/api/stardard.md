<!---
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
-->
## 通讯协议

采用 HTTP 协议，所有数据发送和接受以 JSON 格式进行传递。

## 以资源为中心的 URL 设计

```
/flows/
/flows/:fid/
/flows/:fid/start/
/flows/:fid/stop/
/flows/:fid/preview/
/flows/:fid/nodes/:nid/
/nodes/:nid/
```

基于 DRF 实现方案

```python
# views.py
class FlowViewSet(ModelViewSet):

    def list(self, request):
        ...
    
    def create(self, request):
        ...
    
    def retrieve(self, request, pk=None):
        ...
    
    def update(self, request, pk=None):
        ...

    def partial_update(self, request, pk=None):
        ...
    
    def destroy(self, request, pk=None):
        ...
    
    @list_route(methods=['get'], url_path='get_link_rules_config') 
    def get_link_rules_config(self, request):
        ...
    
    @detail_route(methods=['post'], url_path='start')
    def start(self, request, pk=None):
        ...

# urls.py
from rest_framework import routers

router = routers.DefaultRouter(trailing_slash=True)
router.register(r'flows', views.FlowViewSet, base_name='flow')
router.register(r'flows/(?P<fid>\d+)/nodes', views.NodeViewSet, base_name='node')

urlpatterns = [
    url(r'^', include(router.urls))
]
```

## 请求方法

| method | 含义 |
| --- | --- |
| GET | 获取资源 |
| POST | 创建资源 |
| PATCH | 更新资源的部分属性 |
| PUT | 替换资源，客户端需要提供新建资源的所有属性 |
| DELETE | 删除资源 |
| GET | 获取资源 |


## 请求参数

* GET 请求参数通过查询字符串传递。

    `curl -i "https://api.github.com/repos/vmg/redcarpet/issues?state=closed"`

* POST, PATCH, PUT, and DELETE 请求参数将以 JSON 格式序列化，作为 body 请求体发送至服务端，同时在 header 中显性定义 `Content-Type:application/json`。

    `curl -d '{"scopes":["public_repo"]}' -H 'Content-Type:application/json' https://api.github.com/authorizations`
    
## 返回内容

所有接口返回内容统一以 200 状态码返回，区分状态请使用 `code` 字段

```json
{
    "result": True,
    "data": {},
    "code": '1500200',
    "message": "ok",
    "errors": {}
}
```

列表接口若传入了page+page_size分页参数，返回结果作为字典放在data参数中，必须要的两个字段， count(int)，results(list)，分别为总数和对应的数据；若未传入分页参数，则data仍以list列表全量返回
```json
{
    "result": true,
    "message": "",
    "code": 200,
    "data": {
        "count": 10,
        "results": [
            {
                "id": 1,
                "name": "test1"
            }
        ]
    }
}
```

* result：表示接口处理结果
* data：具体的返回内容，不同接口按照实际情况返回
* code：状态码（错误码），正常情况下默认为 1500200，异常时为有定义的错误码
* message：信息内容，异常时，对错误码做具体的解释
* errors：异常时，返回有结构的错误内容，比如参数校验失败，说明请求参数的错误情况

### 关于什么情况 result=True | False

目前需要团队内各模块统一标准，按照实际场景来划分

1. 获取某一资源，资源不存在，返回 result=False，同时返回错误码申明错误原因
2. 在某一环节出现非正常情况，返回 result=False，同时返回错误码申明错误原因


## Code标准（异常控制）

* HTTP-CODE
    * 4XX -> 15004XX
    * 5XX -> 15005XX
    
* BKDATA-CODE 15XXXXX
    * 15 数据平台
    * xx 模块
    * xxx 模块内错误码
    

错误码说明请求查看平台错误码文档