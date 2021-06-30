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

# BkBase python DataLake SDK
### 运行要求
* python >= 3.5
* jdk 1.8
* python和jdk同为32位，或同为64位

### 编译包并上传
在此目录下调用命令生成安装包，上传到公司的PyPI服务
```bash
python setup.py sdist upload -r xxx
```

### 安装包
指定pip的index-url为公司软件源内部PyPI服务地址，然后使用pip命令安装
```bash
pip install tencent-bkbase-datalake=x.x.x
```


### 运行测试用例
在本地安装pytest和pytest-cov，通过如下命令执行测试用例，并在htmlcov目录中的index.html文件里查看代码覆盖率
```bash
pytest -p no:faulthandler -s --cov=./ --cov-report=html
```


### 使用
参考样例：
```python
from jpype.types import JInt, JLong
from tencent_bkbase_datalake import tables

# 从storekit接口中获取读取iceberg表所需的配置信息，调用sdk方法加载表对象
db_name = "iceberg_591"
table_name = "test_591"
props = {
    "hive.metastore.uris": "thrift://xx.xx.xxx.xx:9083,thrift://xx.xx.xxx.xxx:9083",
    "hive.metastore.warehouse.dir": "/data/iceberg/warehouse",
    "fs.defaultFS": "hdfs://xxxx",
}

# 启动jvm，可以指定jvm的参数
tables.start_jvm_on_need(["-Xms256M", "-Xmx2G"])
table = tables.load_table(db_name, table_name, props)

# 获取字段列表（一维数组）和待写入的数据（二维数组）
column_names = ["col1_int", "col2_long", "dtEventTimeStamp", "dtEventTime"]
column_data = [
    [JInt(int(11000)), JLong(int(120000)), JLong(int(1600934054000)), "2020-09-24 15:54:14"],
    [JInt(int(21000)), JLong(int(220000)), JLong(int(1600934052000)), "2020-09-24 15:54:12"],
    [JInt(int(31000)), JLong(int(320000)), JLong(int(1600934049000)), "2020-09-24 15:54:09"],
    [JInt(int(41000)), JLong(int(420000)), JLong(int(1600934047000)), "2020-09-24 15:54:07"],
    [JInt(int(51000)), JLong(int(520000)), JLong(int(1600934041000)), "2020-09-24 15:54:01"]
]

# 调用sdk中方法写入数据
tables.append_table(table, column_names, column_data, {"committer": "xxx", "info": "aa"})

# 按照分区删除数据，默认表使用dtEventTime字段按照小时分区，分区时间字符串使用UTC时区的时间。
# 例如：2020-09-24-07即UTC时间 ["2020-09-24T07:00:00Z", "2020-09-24T08:00:00Z")
metric = tables.delete_table_partitions(table, tables.ET, ["2020-09-24-07"])

# 按照条件进行数据删除测试，可指定多个条件，通过and/or合并查询条件
expr1 = tables.build_field_condition_expr("col1_int", tables.INT, ">=", 30000)
expr2 = tables.build_field_condition_expr("col2_long", tables.LONG, "<=", 1000000)
expr3 = tables.build_or_condition(expr1, expr2)
metric1 = tables.delete_table_records(table, expr3)
print(metric1)

# 注意时间格式必须为严格的java ISO_DATE_TIME 格式。
# 例如：'2011-12-03T10:15:30Z', '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30+01:00[Europe/Paris]'
start_time = "2020-09-24T10:00:00+00:00"
end_time = "2020-09-25T00:00:00Z"
# 查询表中指定时间段的数据量
cnt = tables.record_count_between(table, start_time, end_time)

# 按照时间条件检索数据并删除
expr4 = tables.build_field_condition_expr(tables.ET, tables.STRING, ">=", start_time)
expr5 = tables.build_field_condition_expr(tables.ET, tables.STRING, "<", end_time)
expr6 = tables.build_and_condition(expr4, expr5)
metric2 = tables.delete_table_records(table, expr6)
print(metric2)

# 按照条件更新数据
expr7 = tables.build_field_condition_expr("col1_int", tables.INT, "<", 30000)
metric3 = tables.update_table(table, expr7, {"col1_int": JInt(888), "col2_long": JLong(99999999)})
print(metric3)

# 遍历表里符合查询条件的数据
cnt = 0
for record in tables.scan_table(table, condition_expr=expr3):
    cnt += 1

# 遍历表里所有的数据，返回指定的字段
idx = 1
for record in tables.scan_table(table, column_names=["col1_int", "dtEventTimeStamp", "dtEventTime"]):
    print(">> %s record: %s" % (idx, record))
    if idx == 10:
        break
    else:
        idx += 1

# truncate表
tables.truncate_table(table)

```
