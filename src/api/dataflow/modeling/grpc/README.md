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

* 本目录下的文件仅用于调用GRPC模型评估服务

* 目录下文件说明如下：

  * direct_access.proto：pb文件，用于描述相关服务请求时的请求体

  * direct_access_pb2.py与direct_access_pb2_grpc.py：这两个文件为根据proto文件自动生成，请勿进行手动的变更。

  * 自动生成方式如下：

    1. 请先安装protobuf，源码地址为：[https://github.com/google/protobuf](https://github.com/protocolbuffers/protobuf)，也可以结合系统下载免安装版本

    2. 安装python相关依赖(注意我们目前使用的是python2)：

       ```
       protobuf==3.14.0
       grpcio==1.27.1
       grpcio-tools==1.26.0
       ```

   3. 编译：

      ```python 
      python2 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. direct_access.proto
      ```

      即可生成direct_access_pb2.py与direct_access_pb2_grpc.py
