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

## 编写单元测试

#### 单元测试配置
- 所有单元测试用例都放在子模块(dataflow)下的`tests`目录中
- **tests**中的`conftest.py`文件, 可以在配置测试用例用到的**fixtures**, 也可以在文件中全局地Patch一些外部依赖 （对于需要进行元数据同步的表, 框架中的`tests/conftest.py`中已经提供了`patch_meta_sync`函数, 可直接import到子模块项目的conftest中使用, 后续会在框架中提供更多通用的fixture以供个模块调用）
- 配置单元测试的外部依赖, 对于需要与外部组件进行连接的测试用例，可以在`tests/tests_conf/local_dataapi_settings.py`和`tests/tests_conf/ci_dataapi_settings_pro.py`中配置外部组件的连接参数, 
配置的组件保证能从对应环境的机器进行连接即可，其中`tests/tests_conf/local_dataapi_settings.py`为本地开发环境配置，`tests/tests_conf/ci_dataapi_settings_pro.py`为gitlab ci自动化测试使用。
- 测试命名规范如下：
```
    - test_dir # 测试目录以 test 开头
    - test_file.py # 测试文件以 test 开头
    - TestClass # 测试类以 Test 开头
    - test_method # 测试方法以 test 开头
```


## 执行单元测试

#### 本地执行
- 通过 `pip` 安装 `dataflow/01_requirements.txt, dataflow/02_requirements_local.txt`中的依赖包
- 在 `pizza/pytest.ini` 最后增加一行 `testpaths=dataflow/tests`
- 使用 `dataflow/tests/tests_conf/local_dataapi_settings.py` 替换本地 `conf/dataapi_settings.py`
- 运行dataflow下全部单元测试： 在 pizza 项目根目录下执行 `pytest -s -c pytest.ini --cov=dataflow`
- 运行单个单元测试： 在 pizza 项目根目录下执行 `pytest dataflow/tests/test_stream/test_job/test_script.py::TestClass::test_method`

#### ci 执行
- Gitlab CI默认会在**Push**代码到非**unification**分支的时候执行单元测试, 执行过程中的日志可以在`Pipelines > Pipelines`中查看
- 每次发起`Merge Request`时，会自动触发一次`Pipeline`，同样触发一次单元测试，测试结果和简单的报告可以在`Merge Request`界面进行查看
