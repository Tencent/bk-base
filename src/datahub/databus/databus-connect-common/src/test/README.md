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

覆盖率测试运行步骤：

注意：运行测试用例前，先确保如下环境已配置
1、jdk 1.8.0_171
2、maven 3.5.4

##在本地环境下运行测试

###一、安装docker
```
>上docker官网下载 https://www.docker.com
```

###二、启动依赖环境
>使用docker-compose启动测试所依赖的环境:
```
docker-compose -f /your/code/path/databus.yml up -d
```

###三、创建influxdb测试库
>使用http api创建mydb
```
curl -XPOST 'http://localhost:8086/query' --data-urlencode 'q=CREATE DATABASE "mydb"'
```

###四、运行测试
>启动测试，在项目pom.xml文件所在目录执行如下命令
```
mvn clean install
```

###四、安装打包（跳过测试）
>启动测试，在项目pom.xml文件所在目录执行如下命令
```
mvn clean install -Dmaven.jacoco.skip=true -Dmaven.test.skip=true
```





