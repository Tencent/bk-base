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
from functools import reduce
from deepdiff import DeepDiff

from django.db import transaction
from django.db.models import Q

from datamanage.pro.datamodel.models.datamodel import (
    DmmModelUserOperationLog,
    DmmModelInfo,
    DmmModelField,
    DmmModelRelation,
)
from datamanage.pro.datamodel.models.indicator import DmmModelIndicator, DmmModelCalculationAtom
from datamanage.pro.datamodel.dmm.object import get_object, models_object_to_dict, get_unique_key
from datamanage.pro.datamodel.dmm.data_model_diff import (
    parse_datamodel_diff_ret,
    get_data_model_object_content_list,
    set_object_id_and_type,
    get_diff_objects_and_diff_result,
)
from datamanage.pro.datamodel.models.model_dict import (
    DataModelObjectOperationType,
    DataModelObjectType,
    DATA_MODEL_OBJECT_TYPE_ALIAS_MAPPINGS,
    DATA_MODEL_OBJECT_OPERATION_TYPE_ALIAS_MAPPINGS,
    IGNORE_KEYS,
)


class ModelReleaseDescription(object):
    # 模型发布描述
    MODELS_LIST = [DmmModelInfo, DmmModelField, DmmModelCalculationAtom, DmmModelIndicator, DmmModelRelation]
    FIELD_DICT = {}
    for model_cls in MODELS_LIST:
        for f in model_cls._meta.fields:
            FIELD_DICT.update({f.name: f.verbose_name})


class OperationLogManager(object):
    @classmethod
    @transaction.atomic(using='bkdata_basic')
    def create_operation_log(cls, **params):
        """
        创建用户操作流水记录
        :param params: {Dict} 用户操作流水参数
        :return:
        """
        operation_log_field_names = [f.name for f in DmmModelUserOperationLog._meta.fields]
        actual_params = {k: v for k, v in list(params.items()) if k in operation_log_field_names}
        operation_log_object = DmmModelUserOperationLog(**actual_params)
        operation_log_object.save()

    @classmethod
    def get_operation_log_list(cls, model_id, params):
        """
        操作记录列表
        :param model_id: {Int} 模型ID
        :param params: {Dict} 操作记录列表参数字典,示例:
        {
            "conditions": [
                {"key":"object_operation","value":["create"]},
                {"key":"object_type","value":["model"]},
                {"key":"created_by","value":["admin"]},
                {"key":"query","value":["每日道具销售额"]},
            ],
            "page": 1,
            "page_size": 10,
            "start_time":"2021-01-06 00:00:45",
            "end_time":"2021-01-06 20:57:45",
        }
        :return: operation_log_list: {List} 操作记录列表
        """
        # 1) 后台分页参数
        page = params['page']
        page_size = params['page_size']
        limit = page_size
        offset = (page - 1) * page_size

        # 2) 按照搜索条件过滤
        query_or_list = []
        for condition_dict in params['conditions']:
            if condition_dict['key'] == 'query':
                for keyword in condition_dict['value']:
                    query_or_list.append(Q(**{'object_name__icontains': keyword}))
                    query_or_list.append(Q(**{'object_alias__icontains': keyword}))
                    query_or_list.append(Q(**{'object_operation__icontains': keyword}))
                    query_or_list.append(Q(**{'object_id__icontains': keyword}))
                    query_or_list.append(Q(**{'created_by__icontains': keyword}))
            elif condition_dict['key'] == 'object':
                for fuzzy in condition_dict['value']:
                    query_or_list.append(Q(**{'object_name__icontains': fuzzy}))
                    query_or_list.append(Q(**{'object_alias__icontains': fuzzy}))
            else:
                query_or_list.append(Q(**{'{}__in'.format(condition_dict['key']): condition_dict['value']}))

        query = Q()
        if query_or_list:
            query = reduce(lambda x, y: x | y, query_or_list)

        # 3) 起始时间&结束时间
        time_range_params = {}
        start_time = params['start_time']
        end_time = params['end_time']
        if start_time:
            time_range_params['created_at__gte'] = start_time
        if end_time:
            time_range_params['created_at__lte'] = end_time

        # 4) 过滤 & 统计总数 & 分页
        order_by_field_str = '-created_at' if params['order_by_created_at'] == 'desc' else 'created_at'
        operation_log_queryset = (
            DmmModelUserOperationLog.objects.filter(model_id=model_id)
            .filter(query)
            .filter(**time_range_params)
            .order_by(order_by_field_str)
        )
        count = operation_log_queryset.count()
        operation_log_queryset = operation_log_queryset[offset : offset + limit]

        # 5) 补充模型发布描述
        operation_log_list = []
        for operation_log_obj in operation_log_queryset:
            # 模型发布操作记录补充发布描述
            description = ''
            if operation_log_obj.object_operation == DataModelObjectOperationType.RELEASE.value:
                # 获取操作前后的对象列表
                (
                    content_before_change_list,
                    content_after_change_list,
                ) = cls.get_before_and_after_change_conts(operation_log_obj)

                # 操作前后的对象列表diff
                diff_objects = cls.get_operation_log_diff_objects(content_before_change_list, content_after_change_list)
                description = cls.get_model_release_description(diff_objects)
            operation_log_dict = models_object_to_dict(
                operation_log_obj,
                description=description,
                exclude_fields=['content_before_change', 'content_after_change'],
            )
            operation_log_dict['orig_version_id'] = operation_log_obj.content_before_change.get('latest_version', None)
            operation_log_dict['new_version_id'] = operation_log_obj.content_after_change.get('latest_version', None)
            operation_log_list.append(operation_log_dict)

        return {'results': operation_log_list, 'count': count}

    @staticmethod
    def get_model_release_description(diff_objects):
        """
        获取模型发布描述
        :param diff_objects:
        :return:
        """
        desc_list = []
        for diff_obj_dict in diff_objects:
            diff_obj_desc_list = []
            diff_obj_desc_list.append(DATA_MODEL_OBJECT_OPERATION_TYPE_ALIAS_MAPPINGS[diff_obj_dict['diff_type']])
            diff_obj_desc_list.append(DATA_MODEL_OBJECT_TYPE_ALIAS_MAPPINGS[diff_obj_dict['object_type']])
            diff_obj_desc_list.append(
                diff_obj_dict['object_id'].replace('{}-'.format(diff_obj_dict['object_type']), '')
            )
            diff_keys_list = []
            if diff_obj_dict.get('diff_keys'):
                diff_keys_list = [
                    ModelReleaseDescription.FIELD_DICT[diff_key]
                    for diff_key in diff_obj_dict['diff_keys']
                    if diff_key in ModelReleaseDescription.FIELD_DICT
                ]

            # 非主表/ 主表有主表中文名修改
            if diff_obj_dict['object_type'] != DataModelObjectType.MASTER_TABLE.value or (
                diff_obj_dict['object_type'] == DataModelObjectType.MASTER_TABLE.value and diff_keys_list
            ):
                desc_list.append(
                    '{}{}'.format(
                        ''.join([str(desc).decode('utf8') for desc in diff_obj_desc_list]),
                        ', '.join([str(diff_key).decode('utf8') for diff_key in diff_keys_list]),
                    )
                )

            if 'diff_objects' in diff_obj_dict:
                for field_dict in diff_obj_dict['diff_objects']:
                    field_desc_list = []
                    field_desc_list.append(DATA_MODEL_OBJECT_OPERATION_TYPE_ALIAS_MAPPINGS[field_dict['diff_type']])
                    field_desc_list.append(DATA_MODEL_OBJECT_TYPE_ALIAS_MAPPINGS[field_dict['object_type']])
                    field_desc_list.append(field_dict['object_id'].replace('{}-'.format(field_dict['object_type']), ''))
                    field_diff_keys_list = []
                    if field_dict.get('diff_keys'):
                        field_diff_keys_list = [
                            ModelReleaseDescription.FIELD_DICT[diff_key]
                            for diff_key in field_dict['diff_keys']
                            if diff_key in ModelReleaseDescription.FIELD_DICT
                        ]
                    desc_list.append(
                        '{}{}'.format(
                            ''.join([str(desc).decode('utf8') for desc in field_desc_list]),
                            ','.join([str(diff_key).decode('utf8') for diff_key in field_diff_keys_list]),
                        )
                    )
        return '; '.join(desc_list)

    @classmethod
    def get_operator_list(cls, model_id):
        """
        操作者列表
        :param model_id: {Int} 模型ID
        :return: operators_queryset: {queryset} 操作者列表
        """
        operators_queryset = DmmModelUserOperationLog.objects.filter(model_id=model_id).values('created_by').distinct()
        operators = [operator_dict['created_by'] for operator_dict in operators_queryset]
        return {'results': operators}

    @classmethod
    def diff_operation_log(cls, operation_id):
        """
        操作前后diff
        :param operation_id: {Int} 操作ID
        :return:
        """
        # 判断操作记录是否存在
        operation_log_object = get_object(DmmModelUserOperationLog, id=operation_id)

        # 获取操作前后的对象列表
        (
            content_before_change_list,
            content_after_change_list,
        ) = cls.get_before_and_after_change_conts(operation_log_object)

        # 操作前后的对象列表diff
        diff_objects, diff_result_dict = get_diff_objects_and_diff_result(
            content_before_change_list, content_after_change_list
        )

        orig_contents_dict = {
            'objects': content_before_change_list,
        }
        # 拿到操作前的操作者和操作时间
        if content_before_change_list:
            last_operation_log_obj_queryset = DmmModelUserOperationLog.objects.filter(
                model_id=operation_log_object.model_id,
                object_type=operation_log_object.object_type,
                object_id=operation_log_object.object_id,
                id__lt=operation_id,
            ).order_by('-created_at')[:1]
            if last_operation_log_obj_queryset.exists():
                orig_contents_dict.update(
                    {
                        'created_at': last_operation_log_obj_queryset[0].created_at,
                        'created_by': last_operation_log_obj_queryset[0].created_by,
                    }
                )

        new_contents_dict = {
            'objects': content_after_change_list,
            'created_at': operation_log_object.created_at,
            'created_by': operation_log_object.created_by,
        }

        return {
            'orig_contents': orig_contents_dict,
            'new_contents': new_contents_dict,
            'diff': {'diff_objects': diff_objects, 'diff_result': diff_result_dict},
        }

    @staticmethod
    def get_operator_log_content_list(content_dict, object_type, object_id):
        """
        获取操作前/后内容列表
        :param content_dict: {Dict} 操作前/后内容dict
        :param object_type: {String} 操作对象类型
        :param object_id: {String} 操作对象ID
        :return: content_list: {List} 操作前/后内容列表
        """
        content_list = []
        if content_dict:
            content_dict.update({'object_type': object_type, 'object_id': object_id})
            if object_type == DataModelObjectType.MASTER_TABLE.value:
                for field_dict in content_dict.get('fields', []):
                    set_object_id_and_type(field_dict, DataModelObjectType.FIELD.value, field_dict['field_name'])
                for model_relation_dict in content_dict.get('model_relation', []):
                    set_object_id_and_type(
                        model_relation_dict,
                        DataModelObjectType.MODEL_RELATION.value,
                        get_unique_key(model_relation_dict, ['model_id', 'field_name']),
                    )
                # 主表没有字段，比如第一次主表保存之前
                if not content_dict.get('fields', []):
                    content_list = []
                    return content_list

            content_list = [content_dict]
        return content_list

    @classmethod
    def get_before_and_after_change_conts(cls, operation_log_object):
        """
        获取操作前后内容对应的实体列表
        :param operation_log_object:
        :param object_type:
        :param object_id:
        :return:
        """
        object_type = operation_log_object.object_type
        object_id = '{}-{}'.format(operation_log_object.object_type, operation_log_object.object_name)
        content_before_change_dict = operation_log_object.content_before_change
        content_after_change_dict = operation_log_object.content_after_change

        if (
            object_type == DataModelObjectType.MODEL.value
            and operation_log_object.object_operation == DataModelObjectOperationType.RELEASE.value
        ):
            content_before_change_list = get_data_model_object_content_list(content_before_change_dict)
            content_after_change_list = get_data_model_object_content_list(content_after_change_dict)
        else:
            content_before_change_list = cls.get_operator_log_content_list(
                content_before_change_dict, object_type, object_id
            )
            content_after_change_list = cls.get_operator_log_content_list(
                content_after_change_dict, object_type, object_id
            )
        return content_before_change_list, content_after_change_list

    @classmethod
    def get_operation_log_diff_objects(cls, content_before_change_list, content_after_change_list):
        """
        获取操作前后diff_objects（除模型发布操作者以外）
        :param content_before_change_list:
        :param content_after_change_list:
        :return:
        """
        diff_ret = DeepDiff(content_before_change_list, content_after_change_list, ignore_order=True)
        diff_objects = parse_datamodel_diff_ret(diff_ret, content_before_change_list, ignore_keys=IGNORE_KEYS)
        return diff_objects
