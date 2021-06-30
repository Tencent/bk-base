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


from django.utils.translation import ugettext as _
from rest_framework.exceptions import ValidationError

from common.transaction import auto_meta_sync

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.datamodel import DmmModelInfo, DmmModelFieldStage
from datamanage.pro.datamodel.models.indicator import DmmModelCalculationAtom, DmmModelIndicator, DmmModelIndicatorStage
from datamanage.pro.datamodel.models.application import DmmModelInstanceIndicator
from datamanage.pro.datamodel.models.model_dict import (
    FieldCategory,
    TimeField,
    DataModelObjectType,
    DataModelObjectOperationType,
    StreamWindowType,
    SchedulingType,
    BatchWindowType,
    SchedulingContentValidateRange,
)
from datamanage.pro.datamodel.dmm.object import (
    get_object,
    models_object_to_dict,
    check_and_assign_update_content,
    update_data_models_objects,
)
from datamanage.pro.datamodel.dmm.schema import get_schema_list
from datamanage.pro.datamodel.dmm.state_machine import update_data_model_state
from datamanage.pro.datamodel.dmm.operation_log_manager import OperationLogManager
from datamanage.pro.datamodel.handlers.verifier import validate_calculation_content, strip_comment
from datamanage.utils.list_tools import split_list
from datamanage.utils.api.dataflow import DataflowApi


class IndicatorManager(object):
    @staticmethod
    def validate_crt_upd_indicator_params(
        calculation_atom_name, schema_list, aggregation_fields=None, model_id=None, parent_indicator_name=None
    ):
        """
        创建指标参数校验:统计口径、模型、父指标是否存在，聚合字段是否是维度字段
        :param calculation_atom_name: {String} 统计口径名称
        :param schema_list: {List} 主表字段列表
        :param aggregation_fields: {List} 聚合字段名称列表
        :param model_id: {Int} 模型ID
        :param parent_indicator_name: {String} 父指标名称
        :return: calculation_atom_obj: {Object}统计口径object, datamodel_obj: {Object} 模型object
        """
        # 1) 校验统计口径是否存在
        calculation_atom_obj = get_object(DmmModelCalculationAtom, calculation_atom_name=calculation_atom_name)

        # 2) 校验模型是否存在
        if not model_id:
            model_id = calculation_atom_obj.model_id
        datamodel_obj = get_object(DmmModelInfo, model_id=model_id)

        # 3) 校验聚合字段是否是维度字段
        if not aggregation_fields:
            return calculation_atom_obj, datamodel_obj

        dimension_fields = [
            field_dict['field_name']
            for field_dict in schema_list
            if field_dict['field_category'] == FieldCategory.DIMENSION.value
        ]
        if not set(aggregation_fields).issubset(dimension_fields):
            raise dm_pro_errors.AggregationFieldsNotDimensionError()

        # 4) 校验父指标是否存在 & 指标统计口径和父指标统计口径是否一致 & 指标的聚合字段是父指标聚合字段的子集
        if parent_indicator_name:
            # 校验父指标是否存在
            parent_indicator_obj = get_object(DmmModelIndicatorStage, indicator_name=parent_indicator_name)
            # 指标统计口径和父指标统计口径是否一致
            if calculation_atom_name != parent_indicator_obj.calculation_atom_name:
                raise dm_pro_errors.CalculationAtomNotSameError()
            # 指标的维度字段是父指标维度字段的子集
            if isinstance(parent_indicator_obj.aggregation_fields, list):
                if not set(aggregation_fields).issubset(parent_indicator_obj.aggregation_fields):
                    raise dm_pro_errors.IndicatorAggrFieldsNotSubsetError()
        return calculation_atom_obj, datamodel_obj

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def create_indicator(cls, params, bk_username):
        """
        创建指标
        :param params: {dict} 指标创建参数，例如
        {
            "model_id": 1,
            "indicator_name":"item_sales_amt_china_1d",
            "indicator_alias":"国内每天按大区统计道具销售额",
            "description":"国内每天按大区统计道具销售额",
            "calculation_atom_name":"item_sales_amt",
            "aggregation_fields":["channel_name"],
            "filter_formula": "os='android'",
            "scheduling_type": "batch",
            "scheduling_content":{},
            "parent_indicator_name": null,
        }
        :param bk_username: {str} 用户名
        :return: indicator_dict: {dict} 指标详情
        """
        # 判断indicator_name对应的指标是否存在
        cls.validate_indicator_name(params['indicator_name'], params['model_id'])

        # 1) 创建指标参数校验
        # 输入字段列表：当前指标为衍生指标,输入字段为父指标字段;当前指标非衍生指标,输入字段为主表字段
        schema_list = (
            cls.get_indicator_fields(params['parent_indicator_name'], params['model_id'], with_time_field=False)
            if params['parent_indicator_name']
            else get_schema_list(params['model_id'], with_field_details=True)
        )

        # 校验统计口径、模型、父指标是否存在，聚合字段是否是维度字段
        calculation_atom_obj, datamodel_obj = cls.validate_crt_upd_indicator_params(
            params['calculation_atom_name'],
            schema_list,
            params['aggregation_fields'],
            params['model_id'],
            params['parent_indicator_name'],
        )

        # 2）校验指标SQL
        # 过滤注释
        filter_formula = strip_comment(params['filter_formula'])
        # 校验统计SQl、过滤条件和聚合字段
        _, condition_fields = validate_calculation_content(
            datamodel_obj.table_name,
            calculation_atom_obj.calculation_atom_name,
            calculation_atom_obj.field_type,
            calculation_atom_obj.calculation_formula,
            schema_list,
            filter_formula,
            params['aggregation_fields'],
        )

        # 3) 校验是否和模型内已有的指标关键参数一致，如一致则不能创建/修改
        # 关联参数对应的hash值
        params['hash'] = cls.get_indicator_hash(params)
        validate_params = {'model_id': params['model_id']}
        cls.validate_indicator_existed(params, **validate_params)

        # 4) 新增指标记录
        indicator_object = DmmModelIndicatorStage(
            model_id=params['model_id'],
            project_id=datamodel_obj.project_id,
            indicator_name=params['indicator_name'],
            indicator_alias=params['indicator_alias'],
            description=params['description'],
            calculation_atom_name=params['calculation_atom_name'],
            aggregation_fields=params['aggregation_fields'],
            filter_formula=params['filter_formula'],
            condition_fields=condition_fields,
            scheduling_type=params['scheduling_type'],
            scheduling_content=params['scheduling_content'],
            parent_indicator_name=params['parent_indicator_name'],
            hash=params['hash'],
            created_by=bk_username,
            updated_by=bk_username,
        )
        indicator_object.save()

        # 5) 模型构建&发布步骤
        step_id, publish_status = update_data_model_state(
            params['model_id'], cls.create_indicator.__name__, bk_username
        )

        # 6）返回指标信息
        indicator_dict = models_object_to_dict(indicator_object, step_id=step_id, publish_status=publish_status)

        # 7) 增加操作记录
        OperationLogManager.create_operation_log(
            created_by=bk_username,
            model_id=params['model_id'],
            object_type=DataModelObjectType.INDICATOR.value,
            object_operation=DataModelObjectOperationType.CREATE.value,
            object_id=params['indicator_name'],
            object_name=params['indicator_name'],
            object_alias=params['indicator_alias'],
            content_after_change=indicator_dict,
        )
        return indicator_dict

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def update_indicator(cls, indicator_name, params, bk_username):
        """
        修改指标
        :param indicator_name: {str} 指标名称
        :param params: {dict} 指标修改参数,例如
        {
            "indicator_alias":"国内每天按大区统计道具销售额",
            "description":"国内每天按大区统计道具销售额",
            "calculation_atom_name":"item_sales_amt",
            "aggregation_fields":["channel_name"],
            "filter_formula": "os='android'"
            "scheduling_type": "batch",
            "scheduling_content":{}
        }
        :param bk_username: {str} 用户名
        :return: indicator_dict: {dict} 指标详情
        """
        # 1）校验指标是否存在 & 统计口径、模型、父指标是否存在，聚合字段是否是维度字段
        # 校验指标是否存在
        indicator_obj = get_object(DmmModelIndicatorStage, indicator_id=params['indicator_id'])

        # 判断indicator_name对应的指标是否存在
        cls.validate_indicator_name(indicator_name, params['model_id'], indicator_id=params['indicator_id'])

        calculation_atom_name = (
            params['calculation_atom_name']
            if 'calculation_atom_name' in params
            else indicator_obj.calculation_atom_name
        )
        # 指标输入字段列表:当前指标为衍生指标,输入字段为父指标字段;当前指标非衍生指标,输入字段为主表字段
        schema_list = (
            cls.get_indicator_fields(params['parent_indicator_name'], indicator_obj.model_id, with_time_field=False)
            if params['parent_indicator_name']
            else get_schema_list(indicator_obj.model_id, with_field_details=True)
        )

        # 参数校验:统计口径、模型、父指标是否存在，聚合字段是否是维度字段
        calculation_atom_obj, datamodel_obj = cls.validate_crt_upd_indicator_params(
            calculation_atom_name,
            schema_list,
            params.get('aggregation_fields', []),
            indicator_obj.model_id,
            params.get('parent_indicator_name', None),
        )

        # 2) 修改前指标信息
        publish_status = datamodel_obj.publish_status
        orig_indicator_dict = cls.get_indicator_info(indicator_name, indicator_obj.model_id, params)
        # 原指标名称
        orig_indicator_name = indicator_obj.indicator_name
        # 指标名称是否有变更
        indicator_name_has_update = True if orig_indicator_name != indicator_name else False

        # 3）判断指标是否有修改
        has_update = check_and_assign_update_content(
            indicator_obj, params, allow_null_fields=['description', 'filter_formula', 'parent_indicator_name']
        )

        # 有修改内容时，校验指标是否可以被修改
        if has_update and not cls.validate_indicator_can_be_editable(
            orig_indicator_dict, indicator_obj, orig_indicator_dict['has_sub_indicators']
        ):
            raise dm_pro_errors.IndicatorKeyParamsCanNotBeUpdatedError()

        # 4) 修改指标记录
        if has_update:
            # 校验指标SQL
            if 'filter_formula' in params or 'aggregation_fields' in params:
                filter_formula = strip_comment(
                    params['filter_formula'] if 'filter_formula' in params else indicator_obj.filter_formula
                )
                _, condition_fields = validate_calculation_content(
                    datamodel_obj.table_name,
                    calculation_atom_obj.calculation_atom_name,
                    calculation_atom_obj.field_type,
                    calculation_atom_obj.calculation_formula,
                    schema_list,
                    filter_formula,
                    params['aggregation_fields']
                    if 'aggregation_fields' in params
                    else indicator_obj.aggregation_fields,
                )
                indicator_obj.condition_fields = condition_fields

            # 关键参数对应的hash值
            if not indicator_obj.hash:
                indicator_obj.hash = cls.get_indicator_hash(indicator_obj)
            # 校验是否和已经存在的指标关键参数一致，如一致则不能创建/修改
            cls.validate_indicator_existed(indicator_obj, orig_indicator_name, model_id=indicator_obj.model_id)

            # 修改指标
            indicator_obj.updated_by = bk_username
            indicator_obj.save()

            # 更新模型publish_status状态
            _, publish_status = update_data_model_state(
                indicator_obj.model_id, cls.update_indicator.__name__, bk_username
            )

        # 5）返回指标信息
        indicator_dict = models_object_to_dict(
            indicator_obj, step_id=datamodel_obj.step_id, publish_status=publish_status
        )

        # 6) 操作记录
        if has_update:
            # 如果indicator_name有变更，操作记录先删后增
            if indicator_name_has_update:
                OperationLogManager.create_operation_log(
                    created_by=bk_username,
                    model_id=indicator_obj.model_id,
                    object_type=DataModelObjectType.INDICATOR.value,
                    object_operation=DataModelObjectOperationType.DELETE.value,
                    object_id=orig_indicator_name,
                    object_name=orig_indicator_name,
                    object_alias=indicator_dict['indicator_alias'],
                    content_before_change=orig_indicator_dict,
                )
                OperationLogManager.create_operation_log(
                    created_by=bk_username,
                    model_id=indicator_obj.model_id,
                    object_type=DataModelObjectType.INDICATOR.value,
                    object_operation=DataModelObjectOperationType.CREATE.value,
                    object_id=indicator_name,
                    object_name=indicator_name,
                    object_alias=indicator_dict['indicator_alias'],
                    content_after_change=indicator_dict,
                )
            # indicator_name没有变更
            else:
                OperationLogManager.create_operation_log(
                    created_by=bk_username,
                    model_id=indicator_obj.model_id,
                    object_type=DataModelObjectType.INDICATOR.value,
                    object_operation=DataModelObjectOperationType.UPDATE.value,
                    object_id=indicator_name,
                    object_name=indicator_name,
                    object_alias=indicator_dict['indicator_alias'],
                    content_before_change=orig_indicator_dict,
                    content_after_change=indicator_dict,
                )
        return indicator_dict

    @staticmethod
    def get_indicator_key_params_hash(indicator_dict, has_sub_indicators=False):
        """
        获取指标关键参数hash,用于判断指标是否能被编辑
        :param indicator_dict: {dict} 指标内容
        :param has_sub_indicators: {bool} 是否有父指标，默认无父指标
        :return: {int} 关键参数对应的hash值
        """
        # 子指标只需要判断统计口径和父指标
        key_params_str = '{}-{}'.format(
            indicator_dict['calculation_atom_name'], indicator_dict['parent_indicator_name']
        )
        if not has_sub_indicators:
            return hash(key_params_str)
        # 计算类型 & 窗口类型
        key_params_str = '{}-{}-{}'.format(
            key_params_str, indicator_dict['scheduling_type'], indicator_dict['scheduling_content']['window_type']
        )
        # 实时 滚动窗口 统计频率
        if (
            indicator_dict['scheduling_type'] == SchedulingType.STREAM.value
            and indicator_dict['scheduling_content']['window_type'] == StreamWindowType.SCROLL.value
        ):
            key_params_str = '{}-{}'.format(key_params_str, indicator_dict['scheduling_content']['count_freq'])
        # 实时 滑动窗口 窗口长度
        elif (
            indicator_dict['scheduling_type'] == SchedulingType.STREAM.value
            and indicator_dict['scheduling_content']['window_type'] == StreamWindowType.SLIDE.value
        ):
            key_params_str = '{}-{}'.format(
                key_params_str, indicator_dict['scheduling_content'].get('window_time', None)
            )
        # 离线 固定窗口 窗口长度
        elif (
            indicator_dict['scheduling_type'] == SchedulingType.BATCH.value
            and indicator_dict['scheduling_content']['window_type'] == BatchWindowType.FIXED.value
            and 'unified_config' in indicator_dict['scheduling_content']
        ):
            key_params_str = '{}-{}-{}'.format(
                key_params_str,
                indicator_dict['scheduling_content']['unified_config'].get('window_size', None),
                indicator_dict['scheduling_content']['unified_config'].get('window_size_period', None),
            )
        # 指标名称
        key_params_str = '{}-{}'.format(key_params_str, indicator_dict['indicator_name'])
        return hash(key_params_str)

    @classmethod
    def validate_indicator_can_be_editable(cls, orig_indicator_dict, new_indicator_dict, has_sub_indicators=False):
        """
        校验指标是否可以编辑
        :param orig_indicator_dict: {dict} 指标原内容,如果传入的参数是models_obj,则model_to_dict
        :param new_indicator_dict: {dict} 指标新内容,如果传入的参数是models_obj,则model_to_dict
        :param has_sub_indicators: {bool} 指标下面是否有子指标，默认没有子指标
        :return: 可以编辑返回True，否则返回False
        """
        if isinstance(orig_indicator_dict, (DmmModelIndicator, DmmModelIndicatorStage)):
            orig_indicator_dict = models_object_to_dict(orig_indicator_dict)
        if isinstance(new_indicator_dict, (DmmModelIndicator, DmmModelIndicatorStage)):
            new_indicator_dict = models_object_to_dict(new_indicator_dict)
        # 关键参数没有被修改，指标其他内容允许编辑
        return cls.get_indicator_key_params_hash(
            orig_indicator_dict, has_sub_indicators
        ) == cls.get_indicator_key_params_hash(new_indicator_dict, has_sub_indicators)

    @classmethod
    def validate_indicator_existed(cls, indicator_dict, orig_indicator_name=None, **params):
        """
        根据输入的指标参数判断相同关键参数指标是否已经存在，若存在，则不允许用户创建/修改
        :param indicator_dict: {dict} 指标字典
        :param orig_indicator_name: {str} 原指标名称，指标变更时，指标名称可能被修改了
        :param params: {dict} 用于筛选特定范围指标
        :return:
        """
        # 如果传入的参数是models_obj,则model_to_dict
        if isinstance(indicator_dict, (DmmModelIndicator, DmmModelIndicatorStage)):
            indicator_dict = models_object_to_dict(indicator_dict)
        if 'calculation_atom_name' not in indicator_dict:
            raise dm_pro_errors.IndicatorHasNoCorrespondingParamError(
                message_kv={'param_name': 'calculation_atom_name'}
            )
        if 'indicator_name' not in indicator_dict:
            raise dm_pro_errors.IndicatorHasNoCorrespondingParamError(message_kv={'param_name': 'indicator_name'})
        # 原指标名称（指标编辑时，名称会变更）
        if orig_indicator_name is None:
            orig_indicator_name = indicator_dict['indicator_name']
        # 拿到对应calculation_atom_name的所有指标
        indicator_queryset = (
            DmmModelIndicatorStage.objects.filter(calculation_atom_name=indicator_dict['calculation_atom_name'])
            .filter(**params)
            .exclude(indicator_name=orig_indicator_name)
        )
        for indicator_obj in indicator_queryset:
            if cls.compare_indicators(models_object_to_dict(indicator_obj), indicator_dict):
                raise dm_pro_errors.SameIndicatorNotPermittedError(
                    message=_(
                        '不允许创建/修改为关键配置相同的指标，{}和{}'.format(
                            indicator_dict['indicator_name'], indicator_obj.indicator_name
                        )
                    )
                )

    @classmethod
    def validate_ind_existed_in_release(cls, model_id, indicator_list):
        """
        发布时校验全局指标名称相同/关键参数相同参数指标是否已经存在
        :param model_id: 模型ID
        :param indicator_list: 指标列表
        :return:
        :raises IndicatorNameAlreadyExistError: 指标名称已存在
        :raises SameIndicatorNotPermittedError: 指标关键配置相同
        """
        indicator_names = []
        calc_atom_names = []
        for indicator_dict in indicator_list:
            if 'indicator_name' in indicator_dict and indicator_dict['indicator_name'] not in indicator_names:
                indicator_names.append(indicator_dict['indicator_name'])
            if (
                'calculation_atom_name' in indicator_dict
                and indicator_dict['calculation_atom_name'] not in calc_atom_names
            ):
                calc_atom_names.append(indicator_dict['calculation_atom_name'])
        # 1)校验指标名称是否全局唯一
        if DmmModelIndicator.objects.filter(indicator_name__in=indicator_names).exclude(model_id=model_id).exists():
            raise dm_pro_errors.IndicatorNameAlreadyExistError()

        # 2)校验指标关键参数是否全局唯一
        # 拿到对应calc_atom_names的所有指标,exclude indicator_names对应的指标
        indicator_queryset = (
            DmmModelIndicator.objects.filter(calculation_atom_name__in=calc_atom_names)
            .exclude(indicator_name__in=indicator_names)
            .exclude(model_id=model_id)
        )
        for indicator_obj in indicator_queryset:
            for indicator_dict in indicator_list:
                if cls.compare_indicators(models_object_to_dict(indicator_obj), indicator_dict):
                    raise dm_pro_errors.SameIndicatorNotPermittedError(
                        message=_(
                            '不允许创建/修改为关键配置相同的指标，{}和{}'.format(
                                indicator_dict['indicator_name'], indicator_obj.indicator_name
                            )
                        )
                    )

    @staticmethod
    def validate_indicator_name(indicator_name, model_id, **exclude_kwargs):
        """
        校验指标名称是否重复
        :param indicator_name: 指标名称
        :param model_id: 模型ID
        :param exclude_kwargs: exclude参数
        :return:
        """
        if (
            DmmModelIndicatorStage.objects.filter(indicator_name=indicator_name, model_id=model_id)
            .exclude(**exclude_kwargs)
            .exists()
        ):
            raise dm_pro_errors.IndicatorNameAlreadyExistError(_('当前指标({})已存在，请勿重复提交'.format(indicator_name)))

    @staticmethod
    def cmp_indicators_without_hash_attr(indicator_dict1, indicator_dict2):
        """
        当指标没有hash值的时候，通过比较IndicatorHashDict，比较两个指标的内容是否一致
        :param indicator_dict1: {Dict}指标一内容
        :param indicator_dict2: {Dict}指标二内容
        :return: 相同则返回True，否则返回False
        """
        indicator_hash_dict1 = IndicatorHashDict(indicator_dict1)
        indicator_hash_dict2 = IndicatorHashDict(indicator_dict2)
        return indicator_hash_dict1 == indicator_hash_dict2

    @classmethod
    def compare_indicators(cls, indicator_dict1, indicator_dict2):
        """
        比较两个指标的内容是否一致
        :param indicator_dict1: {Dict}指标一内容，如果传入的参数是models_obj,则model_to_dict
        :param indicator_dict2: {Dict}指标二内容，如果传入的参数是models_obj,则model_to_dict
        :return: 相同则返回True，否则返回False
        """
        # 如果传入的参数是models_obj,则model_to_dict
        if isinstance(indicator_dict1, (DmmModelIndicator, DmmModelIndicatorStage, DmmModelInstanceIndicator)):
            indicator_dict1 = models_object_to_dict(indicator_dict1)
        if isinstance(indicator_dict2, (DmmModelIndicator, DmmModelIndicatorStage, DmmModelInstanceIndicator)):
            indicator_dict2 = models_object_to_dict(indicator_dict2)

        # 如果hash都不为None,比较hash是否相等
        if indicator_dict1.get('hash', None) and indicator_dict2.get('hash', None):
            return indicator_dict1['hash'] == indicator_dict2['hash']

        # 如果hash中有None,比较两个指标的内容是否一致
        return cls.cmp_indicators_without_hash_attr(indicator_dict1, indicator_dict2)

    @staticmethod
    def get_indicator_hash(indicator_dict):
        """
        设置指标关键参数对应的hash值
        :param indicator_dict: {dict}指标内容，如果传入的参数是models_obj,则model_to_dict
        :return: hash {str}:指标关键参数对应的hash值
        """
        if isinstance(indicator_dict, (DmmModelIndicator, DmmModelIndicatorStage)):
            indicator_dict = models_object_to_dict(indicator_dict)
        indicator_hash_dict = IndicatorHashDict(indicator_dict)
        indicator_hash = str(indicator_hash_dict.get_hash())
        return indicator_hash

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def delete_indicator(cls, indicator_name, model_id, bk_username):
        """
        删除指标
        :param indicator_name: {str} 指标名称
        :param model_id: {int} 模型ID
        :param bk_username: {str} 用户名
        :return: {dict}:模型完成步骤 & 发布状态信息
        """
        # 1）校验指标是否存在，校验指标下面是否有子指标
        indicator_obj = get_object(DmmModelIndicatorStage, indicator_name=indicator_name, model_id=model_id)
        # 指标下面有子指标，则无法删除
        if not cls.is_indicator_deletable(indicator_name, model_id):
            raise dm_pro_errors.IndicatorWithSubIndicatorsCanNotBeDeletedError()

        orig_indicator_dict = cls.get_indicator_info(indicator_name, model_id)
        indicator_obj.delete()

        # 2) 获取当前指标所在的model_id和datamodel_obj
        datamodel_obj = get_object(DmmModelInfo, model_id=model_id)

        # 3) 更新模型publish_status状态
        step_id, publish_status = update_data_model_state(
            datamodel_obj.model_id, cls.delete_indicator.__name__, bk_username
        )

        # 4) 操作记录
        OperationLogManager.create_operation_log(
            created_by=bk_username,
            model_id=indicator_obj.model_id,
            object_type=DataModelObjectType.INDICATOR.value,
            object_operation=DataModelObjectOperationType.DELETE.value,
            object_id=indicator_name,
            object_name=indicator_name,
            object_alias=indicator_obj.indicator_alias,
            content_before_change=orig_indicator_dict,
        )

        return {'step_id': step_id, 'publish_status': publish_status}

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def delete_all_indicators_in_model(cls, model_id):
        """
        删除模型中所有的指标
        :param model_id: {Int} 模型ID
        :return:
        """
        # 由于元数据同步不支持queryset的删除，只能在for循环中删除
        indicator_queryset = DmmModelIndicator.objects.filter(model_id=model_id)
        for indicator_obj in indicator_queryset:
            indicator_obj.delete()
        indicator_stage_queryset = DmmModelIndicatorStage.objects.filter(model_id=model_id)
        for indicator_obj in indicator_stage_queryset:
            indicator_obj.delete()

    @staticmethod
    def get_indicator_tree(indicator_list, parent_indicator_name=None):
        """
        构建指标树形结构
        :param indicator_list: 指标列表
        :param parent_indicator_name: 父指标名称
        :return: indicator_tree_list: {List} 树形结构指标列表
        """
        indicator_tree_list = []
        indicator_tree_dict = {}
        for indicator_dict in indicator_list:
            indicator_dict['indicator_count'] = 0
            indicator_dict['type'] = DataModelObjectType.INDICATOR.value
            indicator_tree_dict[indicator_dict['indicator_name']] = indicator_dict

        # 构建指标树形结构
        for key, value in list(indicator_tree_dict.items()):
            parent_indicator_name_tmp = value['parent_indicator_name']
            if parent_indicator_name_tmp in indicator_tree_dict:
                if 'sub_indicators' not in indicator_tree_dict[parent_indicator_name_tmp]:
                    indicator_tree_dict[parent_indicator_name_tmp]['sub_indicators'] = []
                    indicator_tree_dict[parent_indicator_name_tmp]['indicator_count'] = 0
                indicator_tree_dict[parent_indicator_name_tmp]['sub_indicators'].append(value)
                indicator_tree_dict[parent_indicator_name_tmp]['indicator_count'] += 1

            # 返回符合搜索条件的指标树形结构
            if (
                parent_indicator_name
                and indicator_tree_dict[key]['parent_indicator_name'] == parent_indicator_name
                or not indicator_tree_dict[key]['parent_indicator_name']
            ):
                indicator_tree_list.append(value)
        sorted_indicator_tree_list = sorted(indicator_tree_list, key=lambda item: item['updated_at'], reverse=True)
        return sorted_indicator_tree_list

    @classmethod
    def get_all_indicators(cls, parent_indicator_name, indicator_list, with_sub_indicators, **kwargs):
        """
        递归获取parent_indicator_name下面的所有指标
        :param parent_indicator_name: {String} 父指标名称
        :param indicator_list: {List} 指标列表
        :param with_sub_indicators: {bool} 是否返回子指标
        :param kwargs: {Dict} 额外参数
        :return:
        """
        indicator_queryset = DmmModelIndicatorStage.objects.filter(
            parent_indicator_name=parent_indicator_name
        ).order_by('-updated_at')
        for indicator_obj in indicator_queryset:
            if with_sub_indicators:
                cls.get_all_indicators(indicator_obj.indicator_name, indicator_list, with_sub_indicators, **kwargs)
            indicator_list.append(models_object_to_dict(indicator_obj, **kwargs))

    @classmethod
    def get_ind_trees_in_validate_sche_cont(
        cls, model_id, params=None, validate_range=SchedulingContentValidateRange.ALL
    ):
        """
        校验指标调度内容时，获取格式化指标树形结构列表
        :param model_id: {int} 模型ID
        :param params: {dict} 当前指标节点信息
        :param validate_range: {str} all:模型下全部节点，'current':当前节点，'output':当前&下游节点
        :return: {list}: 格式化指标树形结构列表
        """
        if params is None:
            params = {}
        # 1) 分支1：只校验当前指标（指标创建）
        if validate_range == SchedulingContentValidateRange.CURRENT:
            return [params]

        # 2) 获取指标树形结构列表对应的参数
        filter_params = {'model_id': model_id, 'with_sub_indicators': True}
        if validate_range == SchedulingContentValidateRange.OUTPUT:
            # 指标修改，指标名称可能会变，indicator_id获取指标原indicator_name
            indicator_obj = get_object(
                DmmModelIndicatorStage, indicator_id=params['indicator_id'], model_id=params['model_id']
            )
            filter_params['parent_indicator_name'] = indicator_obj.indicator_name

        # 3) 获取指标树形结构列表
        indicator_tree_list, _, _ = cls.get_indicator_list(filter_params)

        # 4) 分支2: 校验当前模型下所有指标（模型发布）
        if validate_range == SchedulingContentValidateRange.ALL:
            return indicator_tree_list

        # 5) 分支3: 校验当前指标 & 下游指标（指标编辑）
        params['sub_indicators'] = indicator_tree_list
        return [params]

    @classmethod
    def get_ind_sche_cont_in_cur_model(cls, model_id, params=None, validate_range=SchedulingContentValidateRange.ALL):
        """
        获取当前模型下所有的指标/当前指标及其子指标
        :param model_id: {int} 模型ID
        :param params: {dict} 当前指标节点信息
        :param validate_range: {str} all:模型下全部节点，'current':当前节点，'output':当前&下游节点
        :return: scheduling_cont_list {list}: 指标待校验的调度内容列表
        """
        # 1) 获取指标树形结构列表
        indicator_tree_list = cls.get_ind_trees_in_validate_sche_cont(model_id, params, validate_range)

        # 2) 获取平铺的指标列表 & {indicator_name:scheduling_content}mappings
        indicator_list = []
        indicator_scheduling_cont_mappings = {}
        cls.parse_indicator_tree_list(indicator_tree_list, indicator_list, indicator_scheduling_cont_mappings)

        # 3) 获取父指标 & 将父指标调度内容放入{indicator_name:scheduling_content}mappings
        # 指标树形结构对应的parent_indicator_name列表
        parent_indicator_names = list(
            set(
                [
                    indicator_dict['parent_indicator_name']
                    for indicator_dict in indicator_tree_list
                    if indicator_dict['parent_indicator_name']
                ]
            )
        )
        indicator_scheduling_cont_mappings.update(
            {
                parent_indicator_obj.indicator_name: {
                    'scheduling_content': parent_indicator_obj.scheduling_content,
                    'scheduling_type': parent_indicator_obj.scheduling_type,
                }
                for parent_indicator_obj in DmmModelIndicatorStage.objects.filter(
                    model_id=model_id, indicator_name__in=parent_indicator_names
                )
            }
        )

        # 4）获取所有指标的调用内容校验列表
        scheduling_cont_list = []
        for indicator_dict in indicator_list:
            scheduling_cont_dict = {
                'scheduling_content': indicator_dict['scheduling_content'],
                'scheduling_type': indicator_dict['scheduling_type'],
                'from_nodes': [],
                'to_nodes': [],
            }
            if indicator_dict['parent_indicator_name'] and indicator_scheduling_cont_mappings.get(
                indicator_dict['parent_indicator_name']
            ):
                scheduling_cont_dict['from_nodes'].append(
                    indicator_scheduling_cont_mappings[indicator_dict['parent_indicator_name']]
                )
            for sub_indicator_name in indicator_dict['sub_indicator_names']:
                if indicator_scheduling_cont_mappings.get(sub_indicator_name):
                    scheduling_cont_dict['to_nodes'].append(indicator_scheduling_cont_mappings[sub_indicator_name])
            scheduling_cont_list.append(scheduling_cont_dict)
        return scheduling_cont_list

    @classmethod
    def validate_ind_sche_cont_in_cur_model(
        cls, model_id, params=None, validate_range=SchedulingContentValidateRange.ALL
    ):
        """
        发布时校验当前模型下所有指标的调度内容
        :param model_id: {int} 模型ID
        :param params: {dict} 当前指标节点信息
        :param validate_range: {str} all:模型下全部节点，'current':当前节点，'output':当前&下游节点
        :return:
        """
        # 1) 获取指标调度内容列表
        scheduling_cont_list = cls.get_ind_sche_cont_in_cur_model(model_id, params, validate_range)

        # 2) 校验指标调度内容列表
        cls.validate_scheduling_content_list(scheduling_cont_list)

    @classmethod
    def validate_scheduling_content_list(cls, scheduling_cont_list):
        """
        校验指标对应的调度内容列表
        :param scheduling_cont_list: {list} 指标对应的调度内容列表
        :return:
        """
        for scheduling_cont_dict in scheduling_cont_list:
            query_ret = DataflowApi.param_verify(scheduling_cont_dict, raw=True)
            ret = query_ret['result']
            if not ret:
                raise ValidationError(query_ret['message'])

    @classmethod
    def parse_indicator_tree_list(cls, indicator_tree_list, indicator_list, indicator_scheduling_cont_dict):
        """
        递归遍历指标树形结构,用于模型发布时所有指标的校验
        :param indicator_tree_list: {list} 指标树形列表
        :param indicator_list: {list} 指标列表
        :param indicator_scheduling_cont_dict: {dict} 指标调度内容
        :return:
        """
        for indicator_dict in indicator_tree_list:
            indicator_scheduling_cont_dict[indicator_dict['indicator_name']] = {
                'scheduling_type': indicator_dict['scheduling_type'],
                'scheduling_content': indicator_dict['scheduling_content'],
            }
            indicator_dict['sub_indicator_names'] = [
                sub_indicator_dict['indicator_name'] for sub_indicator_dict in indicator_dict.get('sub_indicators', [])
            ]
            indicator_list.append(indicator_dict)
            cls.parse_indicator_tree_list(
                indicator_dict.get('sub_indicators', []), indicator_list, indicator_scheduling_cont_dict
            )

    @classmethod
    def get_indicator_list(cls, params, **kwargs):
        """
        获取指标列表
        :param params: {dict} 参数字典，例如{'calculation_atom_name': 'xx', 'with_sub_indicators': True}
        :param kwargs: {dict} 额外参数，例如{'type':'indicator'}
        :return: {list}: 指标列表, {int}: 模型构建 & 发布完成步骤, {String}: 模型发布状态
        """
        # 1) 参数
        calculation_atom_name = params.get('calculation_atom_name', None)
        parent_indicator_name = params.get('parent_indicator_name', None)
        with_sub_indicators = params.pop('with_sub_indicators', False)
        with_details = params.pop('with_details', [])

        # 2) 模型构建 & 发布完成步骤
        datamodel_obj = get_object(DmmModelInfo, model_id=params['model_id'])
        step_id = datamodel_obj.step_id
        publish_status = datamodel_obj.publish_status

        # 3) 过滤出所有指标
        indicator_list = []
        if parent_indicator_name:
            cls.get_all_indicators(parent_indicator_name, indicator_list, with_sub_indicators, **kwargs)
        else:
            filter_params = {'model_id': params['model_id']}
            if calculation_atom_name:
                filter_params['calculation_atom_name'] = calculation_atom_name
            indicator_queryset = DmmModelIndicatorStage.objects.filter(**filter_params).order_by('-updated_at')
            if calculation_atom_name and not with_sub_indicators:
                indicator_queryset = indicator_queryset.filter(parent_indicator_name=None)
            indicator_list = [models_object_to_dict(indicator_obj, **kwargs) for indicator_obj in indicator_queryset]

        # 4) 聚合字段中文名称 & 指标字段列表
        # 主表字段字典, 用于返回主表聚合字段中文名称
        field_dict = {
            field_obj.field_name: field_obj.field_alias
            for field_obj in DmmModelFieldStage.objects.filter(model_id=params['model_id'])
        }

        calc_atom_names = list(set([indicator_dict['calculation_atom_name'] for indicator_dict in indicator_list]))
        calc_atom_dict = {
            calc_atom_obj.calculation_atom_name: calc_atom_obj.calculation_atom_alias
            for calc_atom_obj in DmmModelCalculationAtom.objects.filter(calculation_atom_name__in=calc_atom_names)
        }
        indicator_alias_dict = {
            indicator_dict['indicator_name']: indicator_dict['indicator_alias'] for indicator_dict in indicator_list
        }

        for indicator_dict in indicator_list:
            # 指标是否可以被删除/编辑
            cls.set_indicator_deletable(indicator_dict)
            # 聚合字段中文名称
            cls.set_aggregation_fields_alias(indicator_dict, field_dict)
            indicator_dict['calculation_atom_alias'] = calc_atom_dict[indicator_dict['calculation_atom_name']]
            # 指标字段列表: 统计口径+聚合字段+时间字段, 用于模型预览展示指标字段
            if 'fields' in with_details:
                cls.set_indicator_fields(indicator_dict, calc_atom_dict, field_dict)
            # 返回去掉注释的sql
            if indicator_dict['filter_formula']:
                indicator_dict['filter_formula_stripped_comment'] = strip_comment(indicator_dict['filter_formula'])
            # 衍生指标返回父指标中文名
            if indicator_dict['parent_indicator_name']:
                indicator_dict['parent_indicator_alias'] = indicator_alias_dict.get(
                    indicator_dict['parent_indicator_name'], None
                )

        # 5) 不需要返回子指标,直接return
        if not with_sub_indicators:
            return indicator_list, step_id, publish_status

        # 6) 需要返回子指标, 构建指标树形结构
        indicator_tree_list = cls.get_indicator_tree(indicator_list, parent_indicator_name=parent_indicator_name)
        return indicator_tree_list, step_id, publish_status

    @classmethod
    def set_indicator_deletable(cls, indicator_dict):
        """
        设置指标是否可以被删除/是否有子指标
        :param indicator_dict: {dict} 指标详情
        :return:
        """
        if 'indicator_name' in indicator_dict and not cls.has_sub_indicators(
            indicator_dict['indicator_name'], indicator_dict['model_id']
        ):
            indicator_dict['deletable'] = True
            indicator_dict['has_sub_indicators'] = False
        else:
            indicator_dict['deletable'] = False
            indicator_dict['has_sub_indicators'] = True

    @classmethod
    def is_indicator_deletable(cls, indicator_name, model_id):
        """
        指标是否可以被删除
        :param indicator_name: {str} 指标名称
        :param model_id: {int} 模型ID
        :return: {bool} 指标是否可以被删除
        """
        return not cls.has_sub_indicators(indicator_name, model_id)

    @staticmethod
    def has_sub_indicators(indicator_name, model_id):
        """
        是否有子指标
        :param indicator_name: {str} 指标名称
        :param model_id: {int} 模型ID
        :return: {bool} 指标是否有子指标
        """
        sub_indicator_queryset = DmmModelIndicatorStage.objects.filter(
            parent_indicator_name=indicator_name, model_id=model_id
        )
        # 指标下面是否有子指标
        return sub_indicator_queryset.exists()

    @staticmethod
    def set_aggregation_fields_alias(indicator_dict, field_dict):
        """
        设置聚合字段中文名
        :param indicator_dict: {Dict} 指标信息
        :param field_dict: {Dict} 主表字段字典
        :return:
        """
        indicator_dict['aggregation_fields_alias'] = [
            field_dict.get(aggr_field_name, None) for aggr_field_name in indicator_dict['aggregation_fields']
        ]

    @staticmethod
    def set_indicator_fields(indicator_dict, calc_atom_dict, field_dict):
        """
        设置指标字段列表:统计口径+聚合字段+时间字段
        :param indicator_dict: {Dict} 指标信息
        :param calc_atom_dict: {Dict} 统计口径名称字典 {calculation_atom_name: calculation_atom_alias}
        :param field_dict: {Dict} 主表字段
        :return:
        """
        # 1）统计口径
        fields = [
            {
                'field_name': indicator_dict['calculation_atom_name'],
                'field_alias': calc_atom_dict[indicator_dict['calculation_atom_name']],
                'field_category': FieldCategory.MEASURE.value,
            }
        ]
        # 2）聚合字段
        fields += [
            {
                'field_name': aggr_field_name,
                'field_alias': field_dict.get(aggr_field_name, None),
                'field_category': FieldCategory.DIMENSION.value,
            }
            for aggr_field_name in indicator_dict['aggregation_fields']
        ]
        # 3）时间字段
        fields.append(TimeField.TIME_FIELD_DICT)
        indicator_dict['fields'] = fields

    @classmethod
    def get_indicator_fields(cls, indicator_name, model_id, with_time_field=False):
        """
        获取指标字段列表:统计口径+聚合字段+时间字段(可选)
        :param indicator_name: {str} 指标名称
        :param model_id: {int} 模型ID
        :param with_time_field: {bool} 是否返回时间字段
        :return:
        """
        # 1) 指标&统计口径详情
        indicator_obj = get_object(DmmModelIndicatorStage, indicator_name=indicator_name, model_id=model_id)
        calc_atom_obj = get_object(DmmModelCalculationAtom, calculation_atom_name=indicator_obj.calculation_atom_name)
        # 2) 主表字段列表
        schema_list = get_schema_list(model_id, with_field_details=True)
        field_dict = {field_dict['field_name']: field_dict for field_dict in schema_list}
        # 3) 获取指标字段列表
        # 统计口径
        fields = [
            {
                'field_name': indicator_obj.calculation_atom_name,
                'field_alias': calc_atom_obj.calculation_atom_alias,
                'field_category': FieldCategory.MEASURE.value,
                'description': calc_atom_obj.description,
                'field_type': calc_atom_obj.field_type,
            }
        ]
        # 聚合字段
        fields += [
            {
                'field_name': aggr_field_name,
                'field_alias': field_dict.get(aggr_field_name, {}).get('field_alias', None),
                'field_category': FieldCategory.DIMENSION.value,
                'description': field_dict.get(aggr_field_name, {}).get('description', None),
                'field_type': field_dict.get(aggr_field_name, {}).get('field_type', None),
            }
            for aggr_field_name in indicator_obj.aggregation_fields
        ]
        # 时间字段
        if with_time_field:
            fields.append(TimeField.TIME_FIELD_DICT)
        return fields

    @classmethod
    def get_indicator_info(cls, indicator_name, model_id, params=None, **kwargs):
        """
        获取指标详情
        :param indicator_name: {String} 指标名称
        :param model_id: {int} 模型ID
        :param params: {Dict} 指标详情参数, 例如{'model_id':1,'with_sub_indicators': True}
        :param kwargs: {Dict} 额外参数，例如{'type':'indicator'}
        :return: indicator_dict:{Dict} 指标详情
        """
        if params is None:
            params = {}
        # 1) 指标详情
        indicator_filter_params = {'model_id': model_id}
        if 'indicator_id' in params:
            indicator_filter_params['indicator_id'] = params['indicator_id']
        # 指标编辑的时候，指标名称可能有变更，有indicator_id，就按照indicator_name做过滤
        else:
            indicator_filter_params['indicator_name'] = indicator_name
        indicator_obj = get_object(DmmModelIndicatorStage, **indicator_filter_params)
        datamodel_obj = get_object(DmmModelInfo, model_id=model_id)
        indicator_dict = models_object_to_dict(indicator_obj, step_id=datamodel_obj.step_id)
        cls.set_indicator_deletable(indicator_dict)

        # 2) 是否返回子指标
        with_sub_indicators = params['with_sub_indicators'] if 'with_sub_indicators' in params else False
        if with_sub_indicators:
            indicator_list = []
            cls.get_all_indicators(indicator_name, indicator_list, with_sub_indicators, **kwargs)
            indicator_tree_list = cls.get_indicator_tree(indicator_list, parent_indicator_name=indicator_name)
            indicator_dict['sub_indicators'] = indicator_tree_list
        return indicator_dict

    @classmethod
    def sync_inds_from_stage_to_master(cls, model_id, indicator_list, bk_username):
        """
        模型发布时，将指标内容写db主表，且按照indicator_id的顺序进行变更
        :return:
        """
        indicator_list = sorted(indicator_list, key=lambda item: item['indicator_id'])
        update_data_models_objects(model_id, DmmModelIndicator, indicator_list, bk_username, ['indicator_name'])


class IndicatorHashDict(dict):
    def __hash__(self):
        """
        获取完整指标参数对应的hash值
        :return: 指标参数对应的hash值
        """
        return hash(
            '{}-{}-{}-{}-{}-{}-{}-{}-{}-{}'.format(
                self['calculation_atom_name'],
                set(split_list(self['aggregation_fields'], ',')),
                strip_comment(self['filter_formula']),
                self['scheduling_type'],
                self['scheduling_content']['window_type'],
                self['scheduling_content']['count_freq'],
                # 离线调度单位
                self['scheduling_content'].get('schedule_period', None),
                # 实时窗口长度
                self['scheduling_content'].get('window_time', None),
                # 离线窗口长度和窗口长度单位
                self['scheduling_content']['unified_config'].get('window_size', None)
                if 'unified_config' in self['scheduling_content']
                else None,
                (
                    self['scheduling_content']['unified_config'].get('window_size_period', None)
                    if 'unified_config' in self['scheduling_content']
                    else None
                ),
            )
        )

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def get_hash(self):
        return self.__hash__()
