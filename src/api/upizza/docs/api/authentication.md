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

蓝鲸基础平台从功能权限和数据权限两个维度进行鉴权，用户权限可以通过在蓝鲸基础平台申请角色获取权限，或者在蓝鲸权限中心申请用户组获取权限

目前存在功能列表，以及角色与用户的关系可以查询权限接口

```
http://localhost:80/v3/auth/auth_page/
```

## 1. 添加鉴权

### 1.1 PIZZA AuthSDK 使用方式

直接调用方法进行校验

```python
from common.auth import check_perm

def check_perm(action_id, object_id=None, bk_username=None, bk_app_code=None, raise_exception=True):
    """
    检查调用方是否具有操作权限，规定了 API 校验流程
    
    @param {String} [object_id] 对象ID
    @param {String} action_id 操作方式
    @param {String} [bk_username] 调用者，若 None 则从环境中获取
    @param {String} [bk_app_code] 调用方 APP CODE，若 None 则从环境中获取
    @param {Boolean} [raise_exception] 校验不通过，是否抛出异常
    
    @paramExample
        {
            object_class: "result_table",
            action_id: "query_data",
            object_id: "11_xxx"
        }
    """

# 权限不足，会自动抛出异常
# 未传递 app_code、username，会从环境变量中获取
check_perm('result_table.query_data', '1_xsx')

# 主动传递 app_code、username
check_perm('result_table.query_data', '1_xsx', bk_username='admin', bk_app_code='data')
```

在 viewset 上加装饰器进行校验

```python
from common.auth import perm_check
from common.views import APIViewSet

"""
perm_check(action_id, detail=True, url_key=None)
支持 viewset 函数的权限校验，从 URL 或者参数中通过 KEY 寻址方式获取获取 object_id

@param {string} action_id 操作方式
@param {string} url_key object_id 在 URL 中的 KEY
@param {boolean} detail 是否存在 object_id
"""


class DemoViewset(APIViewSet):

    @perm_check(action_id='result_table.create')
    @list_route(methods=['get'], url_path='list_method')
    def list_method(self, request, id):
        pass

    @perm_check(action_id='result_table.query_data', detail=False)
    @detail_route(methods=['get'], url_path='detail_method')
    def detail_method(self, request, id):
        pass

    @perm_check(action_id='result_table.query_data', url_key='id2', detail=False)
    @detail_route(methods=['get'], url_path='detail_method')
    def detail_method2(self, request, id, id2):
        """
        所要校验的对象 ID 非 lookup_field 定义的字段
        """
        pass
```

## 2. 请求添加认证信息

对于添加了鉴权的接口，需要将认证参数添加到请求参数中，目前支持三种认证方式（不需要鉴权的接口，不会强要求进行认证）

### 2.1 Inner模式

适用于内部调用模式/测试使用，直接调用原始地址，无需经过 apigateway，其中 bkdata_authentication_method、bk_app_code 和 bk_app_secret 
必传，并且需要与接口服务配置中的（APP_ID，APP_TOKEN）的配置一致。

*简易调用样例*
```
import requests

url = "http://localhost:8000/v3/auth/healthz/"

params = {
    "bkdata_authentication_method": "inner",
    "bk_app_code": "bk_bkdata",
    "bk_app_secret": "xxxxxxx",
    'bk_username': 'xxx',
}

response = requests.request("GET", url, headers=headers, params=params)

print(response.text.encode('utf8'))
```

默认内部调用是不对用户进行鉴权的，如果需要进行鉴权的话，请使用以下方式

```
parameters = {
    'bkdata_authentication_method': 'inner-user',
    'bk_app_code': 'bk_bkdata',
    'bk_app_secret': 'xxxxxxxxxxxxxx',
    'bk_username': 'xxx'
}
```

### 2.2 用户模式

用户认证的入口，由蓝鲸网关转发，在调用蓝鲸网关接口时需要传递有效的应用和用户信息

*网关请求样例*
```
import requests

url = "http://xxx.com/api/c/compapi/data/v3/auth/healthz/"

params = {
    "bk_app_code": "app01",
    "bk_app_secret": "xxxxxxx",
    "bk_token": "xxxxxxxx"
}
response = requests.request("GET", url, headers=headers, params=params)

print(response.text.encode('utf8'))
```

蓝鲸网关默认检查用户认证的信息，只传递 bk_username 将认证失败。如果要跳过用户认证，可以让蓝鲸平台管理员在蓝鲸网关配置应用白名单


通过网关后，转调平台接口，会传递有效的 header 信息如下
```
header = {
    'X-BKAPI-JWT': 'xxxxxxx',
    'GATEWAY-NAME': 'data'
}
```

使用网关的公钥可以解析出 bk_app_code 和 bk_username 进行进一步的鉴权


### 2.3 DataToken模式

需要在平台页面上申请生成 DataToken，仅支持在数据查询相关的接口中使用。同样由蓝鲸网关转发，在调用蓝鲸网关接口时需要传递有效的应用信息。

*网关请求样例*
```
import requests

url = "http://xxx.com/api/c/compapi/data/v3/dataquery/query/"

params = {
    "bk_app_code": "data",
    "bk_app_secret": "xxxxxxx",
    "bkdata_data_token": "fdafadfafdsafafa",
    "bk_username": "user01"
}

response = requests.request("GET", url, headers=headers, params=params)

print(response.text.encode('utf8'))
```

通过网关后，转调平台接口，会传递有效的 header 信息如下，bkdata_data_token 直接透传，
```
header = {
    'X-BKAPI-JWT': 'xxxxxxx',   # API 需要使用网关 bk-data-inner 公钥解析
    'GATEWAY-NAME': 'bk-data-inner',
}
```

使用网关的公钥可以解析出 bk_app_code 和 bk_username 进行进一步的鉴权
