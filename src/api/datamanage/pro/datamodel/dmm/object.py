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
from django.forms.models import model_to_dict

from common.log import logger

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.datamodel import (
    DmmModelInfo,
    DmmModelTop,
    DmmModelField,
    DmmModelFieldStage,
    DmmModelRelation,
    DmmModelRelationStage,
)
from datamanage.pro.datamodel.models.indicator import (
    DmmModelIndicator,
    DmmModelCalculationAtom,
    DmmModelCalculationAtomImage,
    DmmModelIndicatorStage,
    DmmModelCalculationAtomImageStage,
)
from datamanage.pro.datamodel.models.model_dict import OPERATOR_AND_TIME_KEYS, NO_SYNC_ATTRS
from datamanage.pro.datamodel.handlers.time import set_created_at_and_updated_at
from datamanage.pro.datamodel.utils import list_diff_without_same_content


OBJECT_NOT_EXIST_DICT = {
    DmmModelInfo: dm_pro_errors.DataModelNotExistError,  # 模型不存在
    DmmModelTop: dm_pro_errors.DataModelNotTopError,  # 模型尚未置顶
    DmmModelField: dm_pro_errors.FieldNotExistError,  # 字段不存在
    DmmModelFieldStage: dm_pro_errors.FieldNotExistError,  # 字段不存在
    DmmModelIndicator: dm_pro_errors.IndicatorNotExistError,  # 指标不存在
    DmmModelIndicatorStage: dm_pro_errors.IndicatorNotExistError,  # 指标不存在
    DmmModelCalculationAtom: dm_pro_errors.CalculationAtomNotExistError,  # 统计口径不存在
    DmmModelCalculationAtomImage: dm_pro_errors.CalculationAtomImageNotExistError,  # 统计口径引用不存在
    DmmModelCalculationAtomImageStage: dm_pro_errors.CalculationAtomImageNotExistError,  # 统计口径引用不存在
}

MODELS_CLS_IGNORE_KEYS_MAPPINGS = {
    DmmModelField: ['id'] + OPERATOR_AND_TIME_KEYS,
    DmmModelFieldStage: ['id', 'model_id'] + OPERATOR_AND_TIME_KEYS,
    DmmModelRelation: ['id'] + OPERATOR_AND_TIME_KEYS,
    DmmModelRelationStage: ['id', 'model_id'] + OPERATOR_AND_TIME_KEYS,
    DmmModelIndicator: OPERATOR_AND_TIME_KEYS,
    DmmModelCalculationAtomImage: ['id'] + OPERATOR_AND_TIME_KEYS,
}


def get_object(models_cls, use_origin_objects=False, **params):
    """
    判断object是否存在, 存在返回object, 不存在raise相应的异常
    :param models_cls: {Class} models中的class, 例DataModelInfo
    :param params: {Dict} 根据parmas拿到对应的object
    :param use_origin_objects: {Boolean} 是否使用原生objects
    :return: models_object: {Object} models对应的object
    """
    try:
        if not use_origin_objects:
            models_object = models_cls.objects.get(**params)
        else:
            models_object = models_cls.origin_objects.get(**params)
    except models_cls.DoesNotExist as e:
        logger.error('object:{} params:{} not exist error'.format(models_cls, params))
        raise OBJECT_NOT_EXIST_DICT.get(models_cls, e)()
    return models_object


def check_and_assign_update_content(models_object, params, allow_null_fields=None):
    """
    判断models是否需要修改， 并将需要修改的models_object设置为相应的属性值
    :param models_object: {Object} models object
    :param params: {Dict} 参数
    :param allow_null_fields: {List} 允许为null/blank的字段列表,例如description允许改为""
    :return: has_update: {Boolean} models是否有需要修改的内容
    """
    if allow_null_fields is None:
        allow_null_fields = []
    has_update = False
    for key, value in list(params.items()):
        if (value or key in allow_null_fields) and hasattr(models_object, key) and value != getattr(models_object, key):
            setattr(models_object, key, value)
            has_update = True
    return has_update


def is_objects_existed(models_cls, **params):
    """
    判断object是否存在, 存在返回True, 不存在返回False
    :param models_cls: {Class} models中的class, 例DataModelInfo
    :param params: {Dict} 根据params拿到对应的object
    :return: {Boolean}object是否存在 models_queryset:{QuerySet}models对应的queryset
    """
    models_queryset = models_cls.objects.filter(**params)
    if models_queryset.count() > 0:
        logger.info('object:{} params:{} is already existed'.format(models_cls, params))
        return True, models_queryset
    return False, models_queryset


def models_object_to_dict(obj, exclude_fields=None, **params):
    """
    将models object转换为字典
    :param obj: {Object} models对应的object
    :param exclude_fields: {List} 不需要返回的字段
    :param params: {Dict} 其他需要放进字典的属性
    :return: models_dict: {Dict} models object对应的字典
    """
    if exclude_fields is None:
        exclude_fields = []
    models_dict = model_to_dict(obj, exclude=exclude_fields)
    # 创建时间 & 修改时间
    set_created_at_and_updated_at(models_dict, obj)
    # set其他属性
    if params:
        models_dict.update(params)
    return models_dict


def get_unique_key(obj, attr_names):
    """根据attr_names获取实体对应的唯一字符串

    :param obj: 实体内容
    :type obj: object or dict
    :param attr_names: 用于唯一标示实体的属性名列表 ['field_name']
    :type attr_names: list

    :return: 实体对应的唯一字符串，用于区别同类其他object
    :rtype: str
    """
    if isinstance(obj, dict):
        attr_vals = [str(obj[attr_name]) for attr_name in attr_names]
        return '-'.join(attr_vals)
    attr_vals = [str(getattr(obj, attr_name)) for attr_name in attr_names]
    return '-'.join(attr_vals)


def update_objects(
    orig_object_dict, added_objects, updated_objects, deleted_objects, attr_names, bk_username, **specific_attrs
):
    """
    models object_list 修改、新增、删除 (原master_table_manage中代码)
    :param orig_object_dict: {[object]} 数据库已有的字段/模型关联/...列表
    :param added_objects: {[object]} 待新增的object列表
    :param updated_objects: {[object]} 待变更的object列表
    :param deleted_objects: {[object]} 待删除的object列表
    :param attr_names: {List} 用于唯一标示object的属性名列表 ['field_name']
    :param bk_username: {str} 用户名
    :param specific_attrs: {dict} 指定参数字典,用于给object指定属性
    :return:
    """

    def add_models_object(obj):
        """
        新增models记录
        :param obj: {Object} object，待新增object
        :return:
        """
        for key, value in list(specific_attrs.items()):
            setattr(obj, key, value)
        obj.created_by = bk_username
        obj.updated_by = bk_username
        return obj.save()

    def update_models_object(obj):
        """
        修改models记录
        :param obj: {Object} object，待修改object
        :return:
        """
        unique_key = get_unique_key(obj, attr_names)
        for key, value in list(specific_attrs.items()):
            setattr(obj, key, value)
        obj.created_by = orig_object_dict[unique_key].created_by
        obj.created_at = orig_object_dict[unique_key].created_at
        obj.updated_by = bk_username
        if hasattr(orig_object_dict[unique_key], 'id'):
            obj.id = orig_object_dict[unique_key].id
        return obj.save()

    def delete_models_object(obj):
        """
        删除models记录
        :param obj: {Object} object，待删除object
        :return:
        """
        return obj.delete()

    operation_map = {
        'add': add_models_object,
        'update': update_models_object,
        'delete': delete_models_object,
    }

    # 新增/修改/删除object
    has_update = (len(added_objects) + len(updated_objects) + len(deleted_objects)) > 0
    for op_type_objects_list in [
        (['update'] * len(updated_objects), updated_objects),
        (['delete'] * len(deleted_objects), deleted_objects),
        (['add'] * len(added_objects), added_objects),
    ]:
        for (op_type, obj) in zip(*op_type_objects_list):
            operation_map[op_type](obj)
    return has_update


def update_data_models_objects(model_id, models_cls, params_list, bk_username, attr_names, **specific_attrs):
    """
    数据模型models对应的objects进行修改、新增、删除
    :param model_id: {Int} 模型ID
    :param models_cls: {Class} models对应的class,例如DmmModelField
    :param params_list: {List} 参数列表
    :param bk_username: {String} 用户名
    :param attr_names: {List} 用于唯一标示object的属性名列表，例如['field_name']
    :param specific_attrs: {Dict} 指定参数字典
    :return: has_update {bool} 是否有变更
    """
    # 1）获取待新增/删除/变更的object_list
    added_objects, deleted_objects, updated_objects, orig_object_dict = get_added_updated_deleted_objects(
        model_id, models_cls, params_list, attr_names, **specific_attrs
    )

    # 2) 对待新增/删除/变更的object_list分别进行新增、删除、修改
    has_update = update_objects(
        orig_object_dict, added_objects, updated_objects, deleted_objects, attr_names, bk_username, **specific_attrs
    )
    return has_update


def get_added_updated_deleted_objects(model_id, models_cls, params_list, attr_names, **specific_attrs):
    """
    获取待新增/删除/变更的object_list & 原实体列表对应的字典{unique_key: object}
    :param model_id: {Int} 模型ID
    :param models_cls: {Class} models对应的class,例如DmmModelField
    :param params_list: {List} 参数列表
    :param attr_names: {List} 用于唯一标示object的属性名列表，例如['field_name']
    :param specific_attrs: {Dict} 指定参数字典
    :return: has_update {bool} 是否有变更
    """
    # 1) 获取原实体列表
    queryset = models_cls.objects.filter(model_id=model_id).filter(**specific_attrs)
    orig_objects = [obj for obj in queryset]

    # 2) 获取原实体列表对应的字典{unique_key: object}
    orig_object_dict = {}
    for each_object in orig_objects:
        orig_object_dict[get_unique_key(each_object, attr_names)] = each_object

    # 3) 获取待写入db主表的object list
    new_objects = [
        models_cls(**{key: value for key, value in list(params_dict.items()) if key not in NO_SYNC_ATTRS})
        for params_dict in get_actual_params_list(models_cls, params_list)
    ]

    # 4) 获取待新增/删除/变更的object_list
    added_objects, deleted_objects, updated_objects = list_diff_without_same_content(
        orig_objects, new_objects, MODELS_CLS_IGNORE_KEYS_MAPPINGS[models_cls], models_cls
    )
    return added_objects, deleted_objects, updated_objects, orig_object_dict


def get_actual_params_dict(models_cls, params, exclude_attrs=None):
    """
    获取models_cls对应的实际参数字典(过滤掉非models里面的字段)
    :param models_cls: {class} models对应的class,例如DmmModelField
    :param params: {dict} 参数字典
    :param exclude_attrs: {list} exclude的attr列表
    :return: actual_params: {dict} models_cls对应的实际参数字典
    """
    if exclude_attrs is None:
        exclude_attrs = []
    attr_names = get_models_cls_attr_names(models_cls)
    actual_params = {k: v for k, v in list(params.items()) if k in attr_names and k not in exclude_attrs}
    return actual_params


def get_actual_params_list(models_cls, params_list):
    """
    获取models_cls对应的实际参数列表(过滤掉非models里面的字段)
    :param models_cls: {class} models对应的class,例如DmmModelField
    :param params_list: [dict] 参数列表
    :return:
    """
    attr_names = get_models_cls_attr_names(models_cls)
    actual_params_list = [
        {k: v for k, v in list(params_dict.items()) if k in attr_names} for params_dict in params_list
    ]
    return actual_params_list


def get_models_cls_attr_names(models_cls):
    """
    获取models_cls对应的属性列表
    :param models_cls: {class} models对应的class,例如DmmModelField
    :return:
    """
    return [attr.name for attr in models_cls._meta.fields]
