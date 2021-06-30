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

import os

QUERYENGINE_API_ROOT = "http://{host}:{port}/v3/queryengine".format(
    host=os.environ["QUERYENGINE_API_HOST"], port=os.environ["QUERYENGINE_API_PORT"]
)

AUTH_API_ROOT = "http://{host}:{port}/v3/auth".format(
    host=os.environ["AUTH_API_HOST"], port=os.environ["AUTH_API_PORT"]
)

META_API_ROOT = "http://{host}:{port}/v3/meta".format(
    host=os.environ["META_API_HOST"], port=os.environ["META_API_PORT"]
)

DATALAB_API_ROOT = "http://{host}:{port}/v3/datalab".format(
    host=os.environ["DATALAB_API_HOST"], port=os.environ["DATALAB_API_PORT"]
)

DATAFLOW_API_ROOT = "http://{host}:{port}/v3/dataflow".format(
    host=os.environ["DATAFLOW_API_HOST"], port=os.environ["DATAFLOW_API_PORT"]
)

DATAHUB_API_ROOT = "http://{host}:{port}/v3".format(
    host=os.environ["DATAHUB_API_HOST"], port=os.environ["DATAHUB_API_PORT"]
)

JUPYTERHUB_USER = os.environ["JUPYTERHUB_USER"]
