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
from datetime import datetime

import arrow
import attr
import pytz
from jinja2 import Template

from metadata.data_quality import errors
from metadata.data_quality.exc import DataQualityException, DataQualityExceptionByErr
from metadata.data_quality.tools import G_TM
from metadata.type_system.core import MetaData, ReferredMetaData

CATEGORY = 'Consistency Check'


MYSQL_CUSTOM_LOAD_SQL = {
    'TagTarget': "select * from tag_target where target_type in ('data_id', 'table')",
}


def check_consistency():
    """
    用于检查 MetaData 后台 backend 间的数据一致性
    """
    CHECK_ENTITY_LIST = [
        'DataTransferringRelation',
        'ResultTableTypeConfig',
        'JobStatusConfig',
        'ProcessingTypeConfig',
        'ResultTable',
        'TagTarget',
        'StorageResultTable',
        'DataTransferring',
        'DataProcessingRelation',
        'ClusterGroupConfig',
        'AccessRawData',
        'Tag',
        'ResultTableField',
        'StorageClusterConfig',
        'DataProcessing',
        'FieldTypeConfig',
        'ProjectInfo',
        'EncodingConfig',
        'PlatformConfig',
        'ProjectClusterGroupConfig',
        'TimeFormatConfig',
        'BelongsToConfig',
        'TransferringTypeConfig',
        'ContentLanguageConfig',
        'DatabusChannelClusterConfig',
    ]

    for md_type in G_TM.md_types:
        if md_type.__name__ in CHECK_ENTITY_LIST:
            try:
                G_TM.log(CATEGORY, 'Start to check table, md_type={}'.format(md_type), level='info')
                check_entity_consistency(md_type)
            except DataQualityException as exc:
                G_TM.log(CATEGORY, 'Failed to check table, md_type={}, err={}'.format(md_type, exc.message))

    G_TM.log(CATEGORY, 'Final errors statistics: {}'.format(json.dumps(G_TM.errors_statistics)), level='info')


def check_entity_consistency(md_type):
    """
    以 entity 的粒度进行检查一致性
    """
    consistent_fields = []

    # 元素类的主键字段
    identifier = md_type.metadata['identifier'].name

    for field in attr.fields(md_type):
        # :todo 暂不支持校验 lineage_descend 血缘字段和关联对象字段
        if field.name not in ['lineage_descend', 'typed'] and not issubclass(
            field.type, (MetaData, ReferredMetaData, list)
        ):
            consistent_fields.append(field)

    mysql_data = load_data_and_check_schema('mysql', md_type, consistent_fields, identifier)
    dgraph_data = load_data_and_check_schema('dgraph', md_type, consistent_fields, identifier)

    # 数据量必须一致
    if len(mysql_data) != len(dgraph_data):
        G_TM.log_quality_err(
            errors.LengthNotEqualErr(
                md_type=md_type, backend='dgraph', standard_length=len(mysql_data), backend_length=len(dgraph_data)
            )
        )

    m_mysql_data = {d[identifier]: d for d in mysql_data}
    m_dgraph_data = {d[identifier]: d for d in dgraph_data}

    # 以 mysql_data 为准，来校验其他 backend
    for key, mysql_v in list(m_mysql_data.items()):
        if key not in m_dgraph_data:
            G_TM.log_quality_err(errors.EntityNotExistErr(backend='dgraph', md_type=md_type, identifier_value=key))

            continue

        dgraph_v = m_dgraph_data[key]

        for _f in consistent_fields:
            # 仅非 None 值才有比较的必要
            if mysql_v[_f.name] is not None and mysql_v[_f.name] != dgraph_v[_f.name]:
                G_TM.log_quality_err(
                    errors.FieldNotEqualErr(
                        backend='dgraph',
                        md_type=md_type,
                        identifier_value=key,
                        field_name=_f.name,
                        standard_fv=mysql_v[_f.name],
                        backend_fv=dgraph_v[_f.name],
                    )
                )

    return True


def load_data_and_check_schema(backend, md_type, fields, identifier):
    """
    加载数据，并且检查 schema 的合法性

    :param [attr.Attribute] fields
    """
    m_load_func = {'mysql': load_mysql_data, 'dgraph': load_dgraph_data}

    data = m_load_func[backend](md_type, fields, identifier)

    # 检查返回数据是否与要求字段一致
    for d in data:
        identifier_v = d[identifier]
        for _f in fields:
            # 检查字段是否存在
            if _f.name not in d:
                _err = errors.FieldNotExistErr(
                    backend=backend, md_type=md_type, identifier_value=identifier_v, field_name=_f.name
                )

                G_TM.log_quality_err(_err)
                raise DataQualityExceptionByErr(_err)

    return data


def load_mysql_data(md_type, fields, identifier):
    """
    获取对应结果表的数据

    :param [attr.Attribute] fields
    """
    model = G_TM.mysql_backend.get_models_by_md_type(md_type)[0]

    sql = 'select * from {table_name}'.format(table_name=model.__tablename__)

    # 部分 Backend 数据不参与检查
    if md_type.__name__ in MYSQL_CUSTOM_LOAD_SQL:
        sql = MYSQL_CUSTOM_LOAD_SQL[md_type.__name__]

    with G_TM.mysql_backend.operate_session() as ops:
        data = ops.operate(sql)

    m_field = {f.name: f for f in fields}
    field_names = list(m_field.keys())

    for _d in data:
        for _k in list(_d.keys()):
            if _k not in field_names:
                _d.pop(_k)
                continue

            # :todo 寻求一种更好的方式来清洗数据
            if issubclass(m_field[_k].type, (int, float)) and _d[_k] is None:
                _d[_k] = 0
            if issubclass(m_field[_k].type, (str, str)) and _d[_k] is None:
                _d[_k] = ''
            if issubclass(m_field[_k].type, datetime):
                if _d[_k] is not None:
                    _d[_k] = _d[_k].replace(tzinfo=pytz.timezone('Etc/GMT+8'))
                else:
                    _d[_k] = datetime.fromtimestamp(0, pytz.utc)

    return data


QUERY_TABLE_TEMPLATE = Template(
    '''
    {
        target(func: has({{ identifier }})) {
            {% for edge in edges %}{{ edge }}
            {% endfor %}
        }
    }
'''
)


def load_dgraph_data(md_type, fields, identifier):
    """
    获取对应边的数据
    """
    # 整理目标字段与 dgraph 字段的映射表
    schema = {}

    for (_md_type, _md_attr), _dgraph_attr in list(G_TM.dgraph_backend.mappings.items()):
        if _md_type == md_type and _md_attr in fields:
            schema[_dgraph_attr.predicate.name] = _md_attr

    dgraph_identifier = {v.name: k for k, v in list(schema.items())}[identifier]

    statement = QUERY_TABLE_TEMPLATE.render(edges=list(schema.keys()), identifier=dgraph_identifier)

    G_TM.log(CATEGORY, 'Query dgraph backend, statement={}'.format(statement), level='info')

    response = G_TM.dgraph_backend.query(statement)
    data = response['data']['target']

    # 置换 KEY
    for _d in data:
        for _k in list(_d.keys()):
            # :todo 寻求一种更好的方式来清洗数据
            _attr = schema[_k]

            # 将所有时间字符串，全部转换为 UTC 时间对象
            if issubclass(_attr.type, datetime):
                _d[_attr.name] = datetime.fromtimestamp(arrow.get(_d[_k]).timestamp, pytz.utc)
            else:
                _d[_attr.name] = _d[_k]

            if _attr.name != _k:
                _d.pop(_k)

    return data
