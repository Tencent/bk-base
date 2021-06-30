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
import json
import time

from bkbase.dataflow.metrics.meter.counter import Counter
from bkbase.dataflow.metrics.meter.literal import Literal
from bkbase.dataflow.metrics.util.exceptions import MetricsVerifyException
from bkbase.dataflow.metrics.util.json_serialize import ObjectEncoder


class MetricsRegistry(object):
    def __init__(self):
        # metrics 为一个空字典 用户可以上报任意数据
        # 用户可以指定上报的 key, value 为上报内容
        self._metrics = {}

    """
    新增一个 counter/literal 对象
    """

    @staticmethod
    def _new_meter(meter_type):
        if meter_type == "literal":
            return Literal()
        else:
            return Counter()

    """
    判断对象是否为空
    """

    @staticmethod
    def _is_null_meter(meter_object):
        if isinstance(meter_object, Literal):
            return meter_object.get_literal()
        else:
            return meter_object.get_count()

    """
    基本的校验，key是否合法
    """

    @staticmethod
    def _valid_key(key):
        # 1. 非空
        if not key:
            raise MetricsVerifyException(u"添加失败!输入必须为非空")
        # 2. 必须是字符串类型
        if not isinstance(key, str):
            raise MetricsVerifyException(u"添加失败!输入必须为非空字符串类型")
        # 3. 不能以 '.' 开头或者结尾
        if key[0] == "." or key[len(key) - 1] == ".":
            raise MetricsVerifyException(u"添加失败!输入字符不能以'.'开头或结尾")

    """
    1. 路径存在就获取对象,并返回
    2. 路径不存在则创建并返回对象
    返回：meter = Counter/Literal
    """

    def get_or_add(self, key, meter_type):
        self._valid_key(key)
        if "." in key:
            """
            无：增加一个 key, meter_type 对象
            有：返回
            1. a.b.c.d
                直接生成层级关系:a:{b:{c:{d:'meter_type'}}}
            2. 对于无法插入key的情况,抛异常
                例如再插入:a.b.c,抛出异常
            """
            key_list = key.split(".")
            key_except_tail_list = key_list[0 : len(key_list) - 1]
            tail = key_list[len(key_list) - 1]
            temp_dict = self._metrics
            for i in key_except_tail_list:
                if i in temp_dict:
                    temp_dict = temp_dict[i]
                else:
                    temp_dict[i] = {}
                    temp_dict = temp_dict[i]

            """
            路径 a.b.c.{d:xxx} 存在
            添加 a.b.c.d.e = xxx
            """
            # print temp_dict

            if not isinstance(temp_dict, dict):
                raise MetricsVerifyException(u"添加失败!路径%s为非字典类型" % ",".join(key_except_tail_list))
            if tail in temp_dict and self._is_null_meter(temp_dict[tail]):
                """
                key 已经存在且已经被初始化了(不为 None、0、'')
                路径 a.b.c.{d:xxx} 存在
                添加 a.b.c = xxx
                temp_dict[tail] 不是一个 Counter/Literal 对象,是不能返回的
                """
                if isinstance(temp_dict[tail], dict):
                    raise MetricsVerifyException(u"添加失败!路径%s已存在且为字典类型" % ",".join(key_except_tail_list))
                """
                路径 a.b.c.{d:xxx} 存在
                添加 a.b.c.d = xxx,meter 对象存在，直接返回即可
                1. 取的对象与参数不符
                """
                if meter_type == "counter":
                    if not isinstance(temp_dict[tail], Counter):
                        raise MetricsVerifyException(u"取出对象类型与参数不符!对象类型({})参数类型({})".format("Literal", meter_type))
                else:
                    if not isinstance(temp_dict[tail], Literal):
                        raise MetricsVerifyException(u"取出对象类型与参数不符!对象类型({})参数类型({})".format("Counter", meter_type))
                return temp_dict[tail]
            # 路径不存在,新加并返回
            meter = self._new_meter(meter_type)
            temp_dict[tail] = meter
            return meter
        else:
            # 无层级关系
            if key in self._metrics and self._metrics[key]:
                # key 已经存在且已经被初始化了(不为 None、0、'')
                return self._metrics[key]
            else:
                meter = self._new_meter(meter_type)
                self._metrics[key] = meter
                return meter

    def dump_metrics(self):
        # 时间设置为当前时间
        now = int(time.time())
        self.get_or_add("time", "literal").set_literal(now)
        return json.dumps(self._metrics, cls=ObjectEncoder)

    # is_end
    def is_end(self):
        return self.get_or_add("is_end", "literal").get_literal()

    # close 设置上报结束
    def set_end(self):
        self.get_or_add("is_end", "literal").set_literal(True)
