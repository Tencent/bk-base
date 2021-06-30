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


import re
from deepdiff import DeepDiff

from datamanage.pro.datamodel.models.model_dict import (
    DataModelObjectType,
    DataModelObjectOperationType,
    DeepDiffType,
    DIFF_MAPPINGS,
    IGNORE_KEYS,
    FILED_IGNORE_KEYS,
    SCHEDULING_CONTENT_IGNORE_KEYS,
    MODEL_RELATION_IGNORE_KEYS,
)
from datamanage.pro.datamodel.dmm.object import get_unique_key

REGEX = "((root\\[\\d+\\])\\[\\'(\\w+)\\'\\](\\[\\d+\\])?)(\\[\\'(\\w+)\\'\\])?(\\S+)?"
PATTERN = re.compile(REGEX)


def get_diff_objects_and_diff_result(orig_datamodel_object_list, new_datamodel_object_list):
    """获取模型发布/操作前后diff的diff_objects & diff结论

    :param orig_datamodel_object_list: 操作前/上一次发布对象列表
    :type orig_datamodel_object_list: list
    :param new_datamodel_object_list: 操作后/当前对象列表
    :type new_datamodel_object_list: list

    :return: diff_objects, diff_result_dict

        diff_objects 模型发布/操作前后diff的diff_objects
        diff_result_dict diff结论
    """
    # 1) 对模型当前内容和模型上一个发布版本内容做diff
    diff_ret = DeepDiff(orig_datamodel_object_list, new_datamodel_object_list, ignore_order=True)

    # 2) 获取模型的diff_objects
    diff_objects = parse_datamodel_diff_ret(diff_ret, orig_datamodel_object_list, IGNORE_KEYS)

    # 3) 获取模型的diff结论
    diff_result_dict = get_diff_result(diff_objects, new_datamodel_object_list)

    return diff_objects, diff_result_dict


def parse_datamodel_diff_ret(diff_ret, orig_datamodel_object_list, ignore_keys=None):
    """
    解析数据模型diff结果，获取diff_objects
    :param diff_ret: {Dict} 数据模型diff结果
    :param orig_datamodel_object_list: {List} 数据模型操作前/发布前的object_list
    :param ignore_keys: {List} 忽略变更的字段列表
    :return: diff_objects: {List} 有变更的对象列表
    """
    if ignore_keys is None:
        ignore_keys = []
    root = orig_datamodel_object_list

    # 1）解析数据模型diff结果
    diff_object_mappings = {}
    for diff_type in DeepDiffType.get_enum_value_list():
        if diff_type not in diff_ret:
            continue
        # 变更的实体
        if diff_type in [
            DeepDiffType.DICTIONARY_ITEM_ADDED.value,
            DeepDiffType.DICTIONARY_ITEM_REMOVED.value,
            DeepDiffType.TYPE_CHANGES.value,
            DeepDiffType.VALUES_CHANGED.value,
        ]:
            for key in diff_ret[diff_type]:
                # 获取diff的value,从而对field_index变化情况进行分析
                value = diff_ret[diff_type][key] if isinstance(diff_ret[diff_type], dict) else None
                parse_update_diff_ret(root, key, ignore_keys, diff_object_mappings, value)
        # 新增/删除/变更的实体
        if diff_type in [DeepDiffType.ITERABLE_ITEM_ADDED.value, DeepDiffType.ITERABLE_ITEM_REMOVED.value]:
            for key, value in list(diff_ret[diff_type].items()):
                parse_create_delete_update_diff_ret(
                    root, key, value, diff_object_mappings, DIFF_MAPPINGS[diff_type], ignore_keys
                )

    # 2）获取diff_objects
    diff_objects = get_diff_objects(diff_object_mappings, orig_datamodel_object_list)
    return diff_objects


def add_model_upd_objs_to_diff_obj_map(root, key, ignore_keys, diff_object_mappings):  # noqa
    """
    在diff_object_mappings中增加除字段以外的数据模型实体
    :param root: {List} orig_datamodel_object_list
    :param key: {String} 例如"root[0]['fields'][4]['field_constraint_content']['groups'][0]"
    :param ignore_keys: {List} 解析diff的时候允许不一致的字段列表
    :param diff_object_mappings: {Dict} diff_objects映射，{object_id: object_dict}
    :return:
    """
    object_dict, diff_key = get_object_dict_and_diff_key(root, key)
    if not diff_key or diff_key in ignore_keys:
        return

    object_id = object_dict['object_id']
    # 如果当前object不在diff_object_mappings中，将当前object放入diff_object_mappings
    if object_id not in diff_object_mappings:
        diff_keys = [diff_key] if diff_key != 'scheduling_content' else []
        diff_dict = {
            'diff_keys': diff_keys,
            'diff_type': DataModelObjectOperationType.UPDATE.value,
            'object_id': object_id,
            'object_type': object_dict['object_type'],
        }
        diff_object_mappings[object_id] = diff_dict
    # 如果当前object在diff_object_mappings中,更新diff_keys
    elif diff_key not in diff_object_mappings[object_id]['diff_keys']:
        diff_object_mappings[object_id]['diff_keys'].append(diff_key)


def parse_update_diff_ret(root, key, ignore_keys, diff_object_mappings, value=None):
    """解析diff中变更的内容

    :param root: orig_datamodel_object_list
    :type root: list
    :param key: 例如"root[0]['fields'][4]['field_constraint_content']['groups'][0]"
    :type key: str
    :param ignore_keys: 解析diff的时候允许不一致的字段列表
    :type ignore_keys: list
    :param diff_object_mappings: diff_objects映射，{object_id: object_dict}
    :type diff_object_mappings: dict
    :param value: 变更内容
    :type value: str

    :return:
    """
    object_dict, diff_key = get_object_dict_and_diff_key(root, key)
    if not diff_key or diff_key in ignore_keys:
        return

    # 在diff_object_mappings中增加除字段以外的数据模型实体
    add_model_upd_objs_to_diff_obj_map(root, key, ignore_keys, diff_object_mappings)

    # 当前diff_key是fields,在diff_object_mappings中增加变更字段
    if diff_key == '{}s'.format(DataModelObjectType.FIELD.value):
        add_diff_objects_to_diff_object_map(
            root, key, diff_object_mappings, DataModelObjectType.FIELD.value, FILED_IGNORE_KEYS, value
        )
    # 当前diff_key是model_relation,在diff_object_mappings中增加变更模型关联
    elif diff_key == DataModelObjectType.MODEL_RELATION.value:
        add_diff_objects_to_diff_object_map(
            root, key, diff_object_mappings, DataModelObjectType.MODEL_RELATION.value, MODEL_RELATION_IGNORE_KEYS
        )
    # 当前diff_key不是fields，且diff_key后面还有内容，例如['scheduling_content']['advanced']['recovery_enable'],格式化diff_key
    elif diff_key == 'scheduling_content':
        object_id = object_dict['object_id']
        add_scheduling_content_to_diff_keys(key, object_id, diff_object_mappings)


def get_object_dict_and_diff_key(root, key):
    """
    获取diff结果中的object_dict和diff_key
    :param root: {List} orig_datamodel_object_list
    :param key: {String} 例如"root[0]['fields'][4]['field_constraint_content']['groups'][0]"
    :return: object_dict:{Dict}变更的实体字典, diff_key:{String}变更实体对应的变更字段
    """
    object_dict = eval(PATTERN.match(key).group(2))
    diff_key = PATTERN.match(key).group(3)
    return object_dict, diff_key


def add_diff_objects_to_diff_object_map(root, key, diff_object_mappings, object_type, ignore_keys, value=None):  # noqa
    """在diff_object_mappings中增加变更字段/模型关联

    例如字段中field_constraint_content变更，模型关联中related_model_id & related_field_name变更

    :param key: 例如"root[0]['fields'][4]['field_constraint_content']['groups'][0]"
    :type key: str
    :param diff_object_mappings: diff_objects映射，{object_id: object_dict}
    :type diff_object_mappings: dict
    :param object_type: 变更实体,field/model_relation
    :param ignore_keys: 忽略的属性
    :param value: 变更内容
    :type value: str

    :return:
    """
    diff_key = PATTERN.match(key).group(3)
    if diff_key not in ('{}s'.format(DataModelObjectType.FIELD.value), DataModelObjectType.MODEL_RELATION.value):
        return
    object_dict = eval(PATTERN.match(key).group(1))
    object_diff_key = PATTERN.match(key).group(6)
    if not object_diff_key or object_diff_key in ignore_keys:
        return

    # 如果当前object不在diff_object_mappings中，将当前object放入diff_object_mappings
    if object_dict['object_id'] not in diff_object_mappings:
        diff_object_dict = {
            'diff_keys': [object_diff_key],
            'diff_type': DataModelObjectOperationType.UPDATE.value,
            'object_id': object_dict['object_id'],
            'object_type': object_dict['object_type'],
        }
        diff_object_mappings[object_dict['object_id']] = diff_object_dict
    # 如果当前object在diff_object_mappings中,更新diff_keys
    elif object_diff_key not in diff_object_mappings[object_dict['object_id']].get('diff_keys', []):
        diff_object_mappings[object_dict['object_id']]['diff_keys'].append(object_diff_key)

    # 判断字段field_index是变大还是变小，用于标记模型diff的箭头：当field_index变大，字段箭头向下，为down
    if object_type == DataModelObjectType.FIELD.value and object_diff_key == 'field_index':
        diff_object_mappings[object_dict['object_id']]['field_index_update'] = (
            ('down' if value['new_value'] > value['old_value'] else 'up')
            if isinstance(value, dict) and 'new_value' in value and 'old_value' in value
            else None
        )


def add_scheduling_content_to_diff_keys(key, object_id, diff_object_mappings):
    """
    在diff_keys列表中增加格式化调度内容（只有调度内容变更才需要做格式化处理）
    :param key: {String} 例如"root[14]['scheduling_content']['window_lateness']"
    :param object_id: {String}变更的实体ID
    :param diff_object_mappings:{Dict} diff_objects映射，{object_id: object_dict}
    :return:
    """
    regex = "root\\[\\d+\\](\\[\\'\\w+\\'\\]\\S+)?"
    scheduling_content_details = re.match(regex, key).group(1)
    for ignore_key in SCHEDULING_CONTENT_IGNORE_KEYS:
        if ignore_key in scheduling_content_details:
            return
    diff_object_mappings[object_id]['diff_keys'].append(format_diff_key(scheduling_content_details))


def parse_create_delete_update_diff_ret(root, key, value, diff_object_mappings, diff_type, ignore_keys):  # noqa
    """
    解析diff中变更/新增/删除的内容
    :param root: {List} orig_datamodel_object_list
    :param key: {String} 例如"root[0]['fields'][4]['field_constraint_content']['groups'][0]"
    :param value: {Dict} 变更/新增/删除实体对应的详情
    :param diff_object_mappings: {Dict} diff_objects映射，{object_id: object_dict}
    :param diff_type: {String} 变更类型
    :param ignore_keys: {List} 解析diff的时候允许不一致的字段列表
    :return:
    """
    # diff中新增/删除的实体
    if value and 'object_id' in value and 'object_type' in value:
        add_crt_del_objs_to_diff_obj_map(value, diff_object_mappings, diff_type)

    # diff中变更的实体，例如实体中tags/aggregation_fields/aggregation_fields_alias/fields等内容新增/删除
    else:
        parse_update_diff_ret(root, key, ignore_keys, diff_object_mappings)


def add_crt_del_objs_to_diff_obj_map(value, diff_object_mappings, diff_type):
    """
    将diff中新增/删除实体添加至diff_object_mappings
    :param value: {Dict} 新增/删除实体对应的详情
    :param diff_object_mappings: {Dict} diff_objects映射，{object_id: object_dict}
    :param diff_type: {String} 变更类型
    :return:
    """
    diff_dict = {'diff_type': diff_type, 'object_id': value['object_id'], 'object_type': value['object_type']}
    if value['object_id'] not in diff_object_mappings:
        diff_object_mappings[value['object_id']] = diff_dict


def get_diff_objects(diff_object_mappings, orig_datamodel_object_list):
    """获取diff_objects

    :param diff_object_mappings: 变更对象内容mapping
    :param orig_datamodel_object_list: 操作前/上一次发布内容列表

    :return: diff_objects: diff_objects列表
    """
    # 1)从diff_object_mappings中获取diff_objects
    diff_objects = []
    field_diff_objects = []
    model_relation_diff_objects = []
    master_table_diff_object = None
    for key, value in list(diff_object_mappings.items()):
        if value['object_type'] == DataModelObjectType.FIELD.value:
            if value not in field_diff_objects:
                field_diff_objects.append(value)
        elif value['object_type'] == DataModelObjectType.MODEL_RELATION.value:
            if value not in model_relation_diff_objects:
                model_relation_diff_objects.append(value)
        elif value['object_type'] == DataModelObjectType.MASTER_TABLE.value:
            master_table_diff_object = value
        elif value not in diff_objects:
            diff_objects.append(value)

    # 2)将field_diff_objects放入主表的diff_objects中
    # 如果字段有diff
    if field_diff_objects or model_relation_diff_objects:
        # 如果字段有diff，master_table_diff_object还是None
        if master_table_diff_object is None:
            for datamodel_object_dict in orig_datamodel_object_list:
                if datamodel_object_dict['object_type'] == DataModelObjectType.MASTER_TABLE.value:
                    master_table_diff_object = {
                        'diff_type': DataModelObjectOperationType.UPDATE.value,
                        'object_id': datamodel_object_dict['object_id'],
                        'object_type': datamodel_object_dict['object_type'],
                    }
                    break
        # 将field_diff_objects的内容放在主表对应的object中
        master_table_diff_object['diff_objects'] = field_diff_objects + model_relation_diff_objects
        diff_objects.append(master_table_diff_object)
    # 字段没有diff,但为主表整体非修改
    elif (
        master_table_diff_object is not None
        and master_table_diff_object['diff_type'] != DataModelObjectOperationType.UPDATE.value
    ):
        diff_objects.append(master_table_diff_object)
    return diff_objects


def format_diff_key(s):
    """
    对scheduling_content diff内容在放进diff_keys之前做格式化处理
    :param s: {String} 例如，['scheduling_content']['advanced']['recovery_enable']
    :return: {String} 例如，scheduling_content.advanced.recovery_enable
    """
    return s.replace('][', '.').replace('\'', '').replace('[', '').replace(']', '')


def get_diff_result(diff_objects, new_datamodel_object_list):
    """获取当前模型内容和模型上一个发布版本diff结论

    是否有变更，不同操作类型diff数create/update/delete，实体类型主表/统计口径/主表变更数量
    :param diff_objects: diff_objects列表
    :param new_datamodel_object_list: 模型最新的object列表

    :return: diff_result_dict

        当前模型内容和模型上一个发布版本diff结论
    """
    # 1) 操作类型create/update/delete
    diff_type_list = [
        DataModelObjectOperationType.CREATE.value,
        DataModelObjectOperationType.UPDATE.value,
        DataModelObjectOperationType.DELETE.value,
    ]
    diff_result_dict = {diff_type: 0 for diff_type in diff_type_list}

    # 2) 是否有变更
    diff_result_dict.update({'has_diff': True if diff_objects else False})

    # 3) 主表/统计口径/指标变更
    object_type_list = [
        DataModelObjectType.MASTER_TABLE.value,
        DataModelObjectType.CALCULATION_ATOM.value,
        DataModelObjectType.INDICATOR.value,
    ]
    diff_result_dict.update({object_type: 0 for object_type in object_type_list})
    # 字段变更详情
    # 新增/修改/删除
    diff_result_dict[DataModelObjectType.FIELD.value] = {diff_type: 0 for diff_type in diff_type_list}
    # 字段顺序修改
    diff_result_dict[DataModelObjectType.FIELD.value]['field_index_update'] = 0
    diff_result_dict[DataModelObjectType.FIELD.value]['only_field_index_update'] = 0

    # 4) 获取create/update/delete数量，主表/统计口径/主表变更数量
    for diff_object_dict in diff_objects:
        if diff_object_dict['diff_type'] in diff_type_list:
            diff_result_dict[diff_object_dict['diff_type']] += 1
        if diff_object_dict['object_type'] in object_type_list:
            diff_result_dict[diff_object_dict['object_type']] += 1
            # 字段变更详情：新增/修改/删除/字段顺序修改
            set_field_diff_result(diff_object_dict, diff_result_dict, new_datamodel_object_list)
    # 字段变更数量要去除只有字段中只有field_index变更的
    diff_result_dict[DataModelObjectType.FIELD.value][DataModelObjectOperationType.UPDATE.value] -= diff_result_dict[
        DataModelObjectType.FIELD.value
    ]['only_field_index_update']
    return diff_result_dict


def set_field_diff_result(diff_object_dict, diff_result_dict, new_datamodel_object_list):
    """获取当前字段内容和上一个发布版本的diff结论

    新增数量/变更数量(除去字段中只有field_index变更的)/删除数量/field_index变更数量
    :param diff_object_dict: 模型实体中有变更的详情，diff_objects中的某一个
    :param diff_result_dict: 模型diff结论
    :param new_datamodel_object_list: 模型最新的object列表

    :return:
    """
    # 字段变更详情：新增/修改/删除/字段顺序修改
    if diff_object_dict['object_type'] != DataModelObjectType.MASTER_TABLE.value:
        return
    # 如果主表是第一次创建，所有的字段都是新增
    if 'diff_objects' not in diff_object_dict:
        for datamodel_object_dict in new_datamodel_object_list:
            if datamodel_object_dict['object_type'] == DataModelObjectType.MASTER_TABLE.value:
                diff_result_dict[DataModelObjectType.FIELD.value][DataModelObjectOperationType.CREATE.value] += len(
                    datamodel_object_dict['fields']
                )
                break
        return
    for field_diff_object_dict in diff_object_dict['diff_objects']:
        if (
            field_diff_object_dict['diff_type'] not in diff_result_dict
            or field_diff_object_dict['object_type'] != DataModelObjectType.FIELD.value
        ):
            continue
        diff_result_dict[DataModelObjectType.FIELD.value][field_diff_object_dict['diff_type']] += 1
        if field_diff_object_dict['diff_type'] == DataModelObjectOperationType.UPDATE.value:
            if 'field_index' in field_diff_object_dict['diff_keys']:
                diff_result_dict[DataModelObjectType.FIELD.value]['field_index_update'] += 1
                if len(field_diff_object_dict['diff_keys']) > 1:
                    continue
                # 字段只有field_index变更
                diff_result_dict[DataModelObjectType.FIELD.value]['only_field_index_update'] += 1


def get_data_model_object_content_list(datamodel_dict):
    """
    获取数据模型对象列表，用于获取模型发布/操作记录内容对比时的前后对象列表
    :param datamodel_dict: {Dict} 数据模型信息，格式如：
    {
        "model_id":23,
        "model_detail":{
            "indicators":Array[6],
            "fields":Array[10],
            "model_relation":Array[6],
            "calculation_atoms":Array[11]
        },
        "description":"道具流水1",
        "tags":Array[2],
        "latest_version":"xxxxxxxxxxxxxxxx",
        "table_alias":"道具流水表",
        "publish_status":"published",
        "model_alias":"道具流水表",
        "created_by":"",
        "updated_at":"2020-12-09 21:41:36",
        "table_name":"fact_item_flow_20",
        "active_status":"active",
        "model_type":"fact_table",
        "step_id":5,
        "project_id":3,
        "created_at":"2020-10-14 20:23:52",
        "model_name":"fact_item_flow_20",
        "updated_by":""
    }
    :return: object_list {List} 数据模型对象列表
    """
    object_list = []
    if not datamodel_dict:
        return object_list
    model_detail_dict = datamodel_dict.pop('model_detail')

    # 给字段补充object_type和object_id信息
    for field_dict in model_detail_dict['fields']:
        set_object_id_and_type(field_dict, DataModelObjectType.FIELD.value, field_dict['field_name'])
    # 给模型关联补充object_type和object_id信息
    for model_relation_dict in model_detail_dict['model_relation']:
        set_object_id_and_type(
            model_relation_dict,
            DataModelObjectType.MODEL_RELATION.value,
            get_unique_key(model_relation_dict, ['model_id', 'field_name']),
        )

    # 在object_list中添加模型主表
    master_table_dict = {
        'model_name': datamodel_dict['model_name'],
        'fields': model_detail_dict['fields'],
        'model_relation': model_detail_dict['model_relation'],
    }
    add_object_to_datamodel_object_list(
        master_table_dict, object_list, DataModelObjectType.MASTER_TABLE.value, master_table_dict['model_name']
    )

    # 在object_list中添加模型统计口径
    for calc_atom_dict in model_detail_dict['calculation_atoms']:
        add_object_to_datamodel_object_list(
            calc_atom_dict,
            object_list,
            DataModelObjectType.CALCULATION_ATOM.value,
            calc_atom_dict['calculation_atom_name'],
        )

    # 在object_list中添加模型指标
    for indicator_dict in model_detail_dict['indicators']:
        add_object_to_datamodel_object_list(
            indicator_dict, object_list, DataModelObjectType.INDICATOR.value, indicator_dict['indicator_name']
        )
    return object_list


def set_object_id_and_type(object_dict, object_type, object_name):
    """
    数据模型对象详情中添加node_type和node_id属性
    :param object_dict: {Dict} 对象详情
    :param object_type: {String} 对象类型
    :param object_name: {String} 对象ID
    :return:
    """
    object_dict['object_type'] = object_type
    object_dict['object_id'] = '{}-{}'.format(object_type, object_name)


def add_object_to_datamodel_object_list(object_dict, object_list, object_type, object_name):
    """
    实体信息中添加object_type和object_id属性，并在数据模型对象列表中增加对象信息
    :param object_dict: {Dict} 节点详情
    :param object_list: {List} 节点列表
    :param object_type: {String} 节点类型
    :param object_name: {String} 节点ID
    :return:
    """
    set_object_id_and_type(object_dict, object_type, object_name)
    object_list.append(object_dict)
