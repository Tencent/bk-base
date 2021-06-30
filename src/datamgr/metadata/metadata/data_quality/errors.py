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
"""
元数据质量存在的错误汇总，统一错误管理模块
"""


from metadata.util.i18n import lazy_selfish as _


class QualityErr(object):
    CODE = '00'
    MESSAGE = _("元数据质量问题")
    CATEGORY = 'DataQuality'

    def __init__(self, **metrics):
        """
        :param metrics 指标字典数据，可用于渲染模板字符串
        """
        self.metrics = metrics

    def to_all_metrics(self):
        """
        生成日志指标
        """
        _metrics = {
            'category': self.CATEGORY,
            'message': self.message,
            'index': self.index,
            'code': self.CODE,
            'name': self.name,
        }

        _metrics.update(self.metrics)

        return _metrics

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def index(self):
        """
        生成索引，用于标识唯一某一次报错信息
        """
        return '{}::{}'.format(self.CODE, hash(str(sorted(self.metrics))))

    @property
    def message(self):
        """
        生成可读的错误内容
        """
        message = self.MESSAGE.format(**self.metrics)
        return "[{}::{}::{}] {}".format(self.CATEGORY, self.CODE, self.name, message)


class ConsistencyErr(QualityErr):
    CODE = '10'
    MESSAGE = _("数据一致性问题")
    CATEGORY = 'DataConsistency'


class FieldNotExistErr(ConsistencyErr):
    """
    metrics

    :param {string} backend 元数据后端
    :param {string} md_type 元数据表
    :param {string} identifier_value 元数据表的主键值
    :param {string} field_name 字段名称
    """

    CODE = '11'
    MESSAGE = (
        "MetaData field not exist in the actual backend, "
        "backend={backend}, "
        "md_type={md_type}, "
        "identifier_value={identifier_value}, "
        "field_name={field_name}"
    )


class LengthNotEqualErr(ConsistencyErr):
    """
    metrics

    :param {string} backend 元数据后端
    :param {string} md_type 元数据表
    :param {string} standard_length 标准后端表长度
    :param {string} backend_length 对应后端表长度
    """

    CODE = '12'
    MESSAGE = (
        "Has not the same length referring to standard backend, "
        "backend={backend}, "
        "md_type={md_type}, "
        "standard_length={standard_length}, "
        "backend_length={backend_length}"
    )


class EntityNotExistErr(ConsistencyErr):
    """
    metrics

    :param {string} backend 元数据后端
    :param {string} md_type 元数据表
    :param {string} identifier_value 元数据表的主键值
    """

    CODE = '13'
    MESSAGE = (
        "Entity not exist referring to standard backend, "
        "backend={backend}, "
        "md_type={md_type}, "
        "identifier_value={identifier_value}"
    )


class FieldNotEqualErr(ConsistencyErr):
    """
    metrics

    :param {string} backend 元数据后端
    :param {string} md_type 元数据表
    :param {string} identifier_value 元数据表的主键值
    :param {string} field_name 字段名称
    :param {string} standard_fv 标准后端字段值
    :param {string} backend_fv 对应后端字段值
    """

    CODE = '14'
    MESSAGE = (
        "Has not the same field value referring to standard backend, "
        "backend={backend}, "
        "md_type={md_type}, "
        "identifier_value={identifier_value}, "
        "field_name={field_name}, "
        "standard_fv={standard_fv}, "
        "backend_fv={backend_fv}"
    )
