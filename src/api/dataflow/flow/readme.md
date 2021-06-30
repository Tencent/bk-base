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

# Dataflow.flow 

管理 DataFlow 节点以及节点关系，串联dataid，数据清洗，实时计算，离线计算与入库流程。


## flow models 变更步骤说明
### 生成 migrate 文件
```
python manage.py makemigrations flow
```

### 变更 DB
```
python manage.py migrate --database dataflow flow
```

### 在服务器上与其他两个模块 stream、batch 保持一致，使用 SQL 变更

使用 DJANGO 指令将 migrate 文件翻译为 SQL 语句
```
python manage.py sqlmigrate flow 0001_initial
```

添加至 database/patch_flow.sql 文件中


## 本地测试

1. 测试文件统一放置在 flow/tests 目录下

2. 对于其他模块的调用，在服务器上，由于与其他模块处于一台机器上是直接本地调用。在本地，则是通过网关来调用，在 dataflow/settings.py 文件中有明确的设置；

    ```
    if settings.RUN_MODE in ['LOCAL']:
        DATA_FLOW_APPS = ['flow']
        BASE_API_URL = 'XXXX' # XXXX 请自己配置，不要提交
    else: 
        DATA_FLOW_APPS = ['flow', 'stream', 'batch']
        BASE_API_URL = 'http://{host}:{port}'.format(host=BIND_IP_ADDRESS, 
                                                 port=pizza_port)
    ```
    
3. 测试指令 `python manage.py test dataflow.flow --keepdb`


## API 要求使用 DRF 框架书写

## 编码要求

1. 类中的各类方法，请按照 classmethod、instancemethod、property、staticmethod 次序添加
2. 获取单个元素信息方法，命名为 get_xxxx()，获取元素列表方法，命名为 list_xxx()


