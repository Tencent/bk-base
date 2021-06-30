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
from django.db.models import Count
from django.db.models import Q

from common.log import logger
from common.transaction import auto_meta_sync

from datamanage.pro.datamodel.utils import list_diff
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.datamodel import DmmModelInfo, DmmModelFieldStage
from datamanage.pro.datamodel.models.indicator import (
    DmmModelCalculationAtom,
    DmmModelCalculationAtomImage,
    DmmModelCalculationAtomImageStage,
    DmmCalculationFunctionConfig,
    DmmModelIndicatorStage,
    DmmModelIndicator,
)
from datamanage.pro.datamodel.dmm.object import (
    get_object,
    check_and_assign_update_content,
    is_objects_existed,
    models_object_to_dict,
    update_data_models_objects,
)
from datamanage.pro.datamodel.models.model_dict import (
    CalculationContentType,
    CalculationAtomType,
    AggregateFunction,
    DataModelObjectType,
    DataModelObjectOperationType,
)
from datamanage.pro.datamodel.handlers.verifier import validate_calculation_content, strip_comment
from datamanage.pro.datamodel.dmm.indicator_manager import IndicatorManager
from datamanage.pro.datamodel.dmm.schema import get_schema_list
from datamanage.pro.datamodel.dmm.state_machine import update_data_model_state
from datamanage.pro.datamodel.dmm.operation_log_manager import OperationLogManager


class CalculationAtomManager(object):
    @staticmethod
    def get_calculation_formula(content_dict, option):
        """
        获得统计口径聚合SQL
        :param content_dict: {Dict} 统计内容
        :param option: {String} 统计内容提交方式
        :return: calculation_formula:{String} 聚合SQL
        """
        # 判断统计方式提交类型：表单/SQL
        if option == CalculationContentType.TABLE.value:
            calculation_formula = AggregateFunction.get_calculation_formula(
                content_dict['calculation_function'], content_dict['calculation_field']
            )
        else:
            calculation_formula = content_dict['calculation_formula']
        return calculation_formula

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def create_calculation_atom(cls, params, bk_username):
        """
        创建统计口径
        :param params: {Dict} 创建统计口径参数，例如
        {
            "model_id": 32,
            "calculation_atom_name": "item_sales_distinct_count",
            "calculation_atom_alias": "item_sales_distinct_count",
            "description": "item_sales_distinct_count",
            "field_type": "long",
            "calculation_content": {
                "option": "TABLE",
                "content": {
                    "calculation_field": "price",
                    "calculation_function": "count_distinct"
                }
            }
        }
        :param bk_username: {String} 用户名
        :return: calculation_atom_dict:{Dict} 统计口径详情
        """
        # 1) 判断model_id对应的数据模型是否存在
        datamodel_object = get_object(DmmModelInfo, model_id=params['model_id'])
        project_id = datamodel_object.project_id

        # 2) 根据统计口径名称判断统计口径是否已经存在
        is_calculation_atom_existed, calc_atom_queryset = is_objects_existed(
            DmmModelCalculationAtom, calculation_atom_name=params['calculation_atom_name']
        )
        if is_calculation_atom_existed:
            raise dm_pro_errors.CalculationAtomNameAlreadyExistError()

        # 3) 统计SQL
        calculation_formula = cls.get_calculation_formula(
            params['calculation_content']['content'], params['calculation_content']['option']
        )
        # 判断该模型下统计口径聚合逻辑是否已经存在
        cls.validate_calc_formula_existed(
            params['model_id'], calculation_formula, DataModelObjectOperationType.CREATE.value
        )

        # 4) 对统计SQL进行校验
        schema_list = get_schema_list(params['model_id'])
        origin_fields_dict, condition_fields = validate_calculation_content(
            datamodel_object.table_name,
            params['calculation_atom_name'],
            params['field_type'],
            calculation_formula,
            schema_list,
        )
        if params['calculation_atom_name'] not in origin_fields_dict:
            raise dm_pro_errors.SqlValidateError(message_kv={'error_info': _('未解析出正确的计算来源字段origin_fields')})
        origin_fields = origin_fields_dict[params['calculation_atom_name']]

        # 5) 新增统计口径记录
        calculation_atom_object = DmmModelCalculationAtom(
            model_id=params['model_id'],
            project_id=project_id,
            calculation_atom_name=params['calculation_atom_name'],
            calculation_atom_alias=params['calculation_atom_alias'],
            description=params['description'],
            field_type=params['field_type'],
            calculation_content=params['calculation_content'],
            calculation_formula=calculation_formula,
            origin_fields=origin_fields,
            created_by=bk_username,
            updated_by=bk_username,
        )
        calculation_atom_object.save()

        # 更新模型publish_status状态
        step_id, publish_status = update_data_model_state(
            params['model_id'], cls.create_calculation_atom.__name__, bk_username
        )

        # 6) 返回统计口径信息
        calculation_atom_dict = models_object_to_dict(
            calculation_atom_object,
            step_id=step_id,
            publish_status=publish_status,
            calculation_formula_stripped_comment=strip_comment(calculation_atom_object.calculation_formula),
        )

        # 7) 操作记录
        OperationLogManager.create_operation_log(
            created_by=bk_username,
            model_id=params['model_id'],
            object_type=DataModelObjectType.CALCULATION_ATOM.value,
            object_operation=DataModelObjectOperationType.CREATE.value,
            object_id=calculation_atom_dict['calculation_atom_name'],
            object_name=calculation_atom_dict['calculation_atom_name'],
            object_alias=calculation_atom_dict['calculation_atom_alias'],
            content_after_change=calculation_atom_dict,
        )
        return calculation_atom_dict

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def update_calculation_atom(cls, calculation_atom_name, params, bk_username):
        """
        修改统计口径
        :param calculation_atom_name: {String} 统计口径名称
        :param params: {Dict} 修改统计口径参数，例如：
        {
            "model_id": 32,
            "description": "item_sales_distinct_count",
            "calculation_content": {
                "option": "SQL",
                "content": {
                    "calculation_formula": "count(distinct price)"
                }
            }
        }
        :param bk_username: {String} 用户名
        :return: calculation_atom_dict:{Dict} 统计口径详情
        """
        # 1) 判断model_id对应的数据模型是否存在
        datamodel_object = get_object(DmmModelInfo, model_id=params['model_id'])
        publish_status = datamodel_object.publish_status

        # 2) 判断统计口径是否存在,防止修改引用统计口径
        calculation_atom_object = get_object(
            DmmModelCalculationAtom, model_id=params['model_id'], calculation_atom_name=calculation_atom_name
        )
        # 获取变更前的统计口径详情
        orig_calculation_atom_dict = cls.get_calculation_atom_info(calculation_atom_name)

        # 3) 统计SQL和格式化统计方式内容
        has_cal_content = 'calculation_content' in params
        if has_cal_content:
            params['calculation_formula'] = cls.get_calculation_formula(
                params['calculation_content']['content'], params['calculation_content']['option']
            )
            # 判断该模型下统计口径聚合逻辑是否已经存在
            cls.validate_calc_formula_existed(
                params['model_id'],
                params['calculation_formula'],
                DataModelObjectOperationType.UPDATE.value,
                calculation_atom_name,
            )

        # 4) 修改统计口径记录
        # 判断模型是否有需要修改的内容
        has_update = check_and_assign_update_content(calculation_atom_object, params, allow_null_fields=['description'])
        # 有需要修改的内容
        if has_update:
            # 如果关键参数不允许修改
            if not cls.is_calatom_deletable_param_editable(params['model_id'], [calculation_atom_name])[
                calculation_atom_name
            ]['key_params_editable'] and not cls.validate_calc_atom_can_be_editable(
                orig_calculation_atom_dict, calculation_atom_object
            ):
                raise dm_pro_errors.CalculationAtomKeyParamsCanNotBeUpdatedError()

            # 统计内容有变更, 对统计SQL进行校验
            if has_cal_content:
                field_type = params['field_type'] if 'field_type' in params else calculation_atom_object.field_type
                schema_list = get_schema_list(params['model_id'])
                origin_fields_dict, condition_fields = validate_calculation_content(
                    datamodel_object.table_name,
                    calculation_atom_name,
                    field_type,
                    params['calculation_formula'],
                    schema_list,
                )
                if calculation_atom_name not in origin_fields_dict:
                    raise dm_pro_errors.SqlValidateError(message_kv={'error_info': _('未解析出正确的计算来源字段origin_fields')})
                calculation_atom_object.origin_fields = origin_fields_dict[calculation_atom_name]
            calculation_atom_object.updated_by = bk_username
            calculation_atom_object.save()

            # 更新模型publish_status状态
            step_id, publish_status = update_data_model_state(
                params['model_id'], cls.update_calculation_atom.__name__, bk_username
            )

        # 5) 返回修改后的统计口径信息
        calculation_atom_dict = models_object_to_dict(
            calculation_atom_object,
            step_id=datamodel_object.step_id,
            publish_status=publish_status,
            calculation_formula_stripped_comment=strip_comment(calculation_atom_object.calculation_formula),
        )

        # 6) 如果统计口径有变更，增加操作记录
        if has_update:
            OperationLogManager.create_operation_log(
                created_by=bk_username,
                model_id=params['model_id'],
                object_type=DataModelObjectType.CALCULATION_ATOM.value,
                object_operation=DataModelObjectOperationType.UPDATE.value,
                object_id=calculation_atom_dict['calculation_atom_name'],
                object_name=calculation_atom_dict['calculation_atom_name'],
                object_alias=calculation_atom_dict['calculation_atom_alias'],
                content_before_change=orig_calculation_atom_dict,
                content_after_change=calculation_atom_dict,
            )
        return calculation_atom_dict

    @classmethod
    def validate_calc_atom_can_be_editable(cls, orig_calc_atom_dict, new_calc_atom_dict):
        """
        校验统计口径是否可以编辑
        :param orig_calc_atom_dict: {dict} 统计口径原内容,如果传入的参数是models_obj,则model_to_dict
        :param new_calc_atom_dict: {dict} 统计口径新内容,如果传入的参数是models_obj,则model_to_dict
        :return: 可以编辑返回True，否则返回False
        """
        if isinstance(orig_calc_atom_dict, DmmModelCalculationAtom):
            orig_calc_atom_dict = models_object_to_dict(orig_calc_atom_dict)
        if isinstance(new_calc_atom_dict, DmmModelCalculationAtom):
            new_calc_atom_dict = models_object_to_dict(new_calc_atom_dict)
        # 部分关键参数不可修改
        return cls.get_calc_atom_key_params_hash(orig_calc_atom_dict) == cls.get_calc_atom_key_params_hash(
            new_calc_atom_dict
        )

    @classmethod
    def get_calc_atom_key_params_hash(cls, calc_atom_dict):
        """
        获取统计口径关键参数对应的hash值
        :param calc_atom_dict: {dict} 统计口径信息
        :return:
        """
        calc_atom_dict['calculation_formula'] = cls.get_calculation_formula(
            calc_atom_dict['calculation_content']['content'], calc_atom_dict['calculation_content']['option']
        )
        return hash('{}-{}'.format(calc_atom_dict['calculation_formula'], calc_atom_dict['field_type']))

    @staticmethod
    def validate_calc_formula_existed(model_id, calc_formula, calc_atom_edit_type, calc_atom_name=None):
        """
        判断统计口径聚合逻辑是否已经存在
        :param model_id: {Int} 模型ID
        :param calc_formula: {String} 统计口径聚合逻辑
        :param calc_atom_edit_type: {String} create/update
        :param calc_atom_name: {String} 统计口径名称
        :return:
        """
        is_calculation_formula_existed, calc_atom_queryset = is_objects_existed(
            DmmModelCalculationAtom, model_id=model_id, calculation_formula=calc_formula
        )
        if not is_calculation_formula_existed:
            return
        if calc_atom_edit_type == DataModelObjectOperationType.CREATE.value or (
            calc_atom_edit_type == DataModelObjectOperationType.UPDATE.value
            and calc_atom_queryset[0].calculation_atom_name != calc_atom_name
        ):
            raise dm_pro_errors.CalculationFormulaAlreadyExistError()

    @staticmethod
    def is_calatom_deletable_param_editable(model_id, calculation_atom_names):
        """
        统计口径列表是否可以被删除:是否被其他模型引用/被指标应用
        :param model_id: {Int} 模型ID
        :param calculation_atom_names: {List} 统计口径名称列表
        :return: calc_atom_deletable_dict: {Dict} 统计口径是否可以删除 & 是否被其他模型引用/被指标应用
        """
        # 判断 1) 统计口径在当前模型下草稿态&发布态是否被指标应用
        calc_atom_deletable_dict = {}
        calc_atom_names_applied_by_indicators_in_stage = list(
            DmmModelIndicatorStage.objects.filter(model_id=model_id, calculation_atom_name__in=calculation_atom_names)
            .values('calculation_atom_name')
            .annotate(count=Count('calculation_atom_name'))
            .values_list('calculation_atom_name', flat=True)
        )
        calc_atom_names_applied_by_indicators = list(
            DmmModelIndicator.objects.filter(model_id=model_id, calculation_atom_name__in=calculation_atom_names)
            .values('calculation_atom_name')
            .annotate(count=Count('calculation_atom_name'))
            .values_list('calculation_atom_name', flat=True)
        )
        applied_calc_atom_names = set(
            list(calc_atom_names_applied_by_indicators_in_stage + calc_atom_names_applied_by_indicators)
        )
        _, _, same_applied_calculation_atom_names = list_diff(applied_calc_atom_names, calculation_atom_names)

        # 判断 2) 统计口径是否被其他模型引用
        # 模型中创建的统计口径
        created_calc_atom_names = [
            calc_atom_dict['calculation_atom_name']
            for calc_atom_dict in DmmModelCalculationAtom.objects.filter(model_id=model_id).values(
                'calculation_atom_name'
            )
        ]
        quoted_calc_atom_queryset = DmmModelCalculationAtomImage.objects.filter(
            calculation_atom_name__in=created_calc_atom_names
        )
        quoted_calc_atom_names = set(
            list([calc_atom_obj.calculation_atom_name for calc_atom_obj in quoted_calc_atom_queryset])
        )
        _, _, same_quoted_calc_atom_names = list_diff(quoted_calc_atom_names, calculation_atom_names)

        # 3) 返回模型是否被引用结果
        for calc_atom_name in calculation_atom_names:
            calc_atom_deletable_dict[calc_atom_name] = {
                'is_applied_by_indicators': False,
                'is_quoted_by_other_models': False,
                'deletable': True,
                'key_params_editable': True,
            }
            if calc_atom_name in same_applied_calculation_atom_names:
                calc_atom_deletable_dict[calc_atom_name].update(
                    {'is_applied_by_indicators': True, 'deletable': False, 'key_params_editable': False}
                )
            if calc_atom_name in same_quoted_calc_atom_names:
                calc_atom_deletable_dict[calc_atom_name].update(
                    {'is_quoted_by_other_models': True, 'deletable': False, 'key_params_editable': False}
                )
        return calc_atom_deletable_dict

    @classmethod
    def set_calatom_deletable_editable(cls, model_id, calculation_atom_names, calc_atom_list):
        """
        设置统计口径是否可以被删除/关键参数是否可以被修改:是否被其他模型引用/被指标应用
        :param model_id: {int} 模型ID
        :param calculation_atom_names: {list} 统计口径名称列表
        :param calc_atom_list: {list} 统计口径列表
        :return:
        """
        calc_atom_deletable_dict = cls.is_calatom_deletable_param_editable(model_id, calculation_atom_names)
        for calc_atom_dict in calc_atom_list:
            calc_atom_dict.update(calc_atom_deletable_dict.get(calc_atom_dict['calculation_atom_name'], {}))

    @classmethod
    def set_calculation_atoms_editable(cls, calc_atom_list):
        """
        设置统计口径是否可以被编辑:当前模型下创建的统计口径可以被编辑，引用的统计口径不可以被编辑
        :param calc_atom_list: {List} 统计口径列表
        :return:
        """
        for calc_atom_dict in calc_atom_list:
            calc_atom_dict['editable'] = (
                True if calc_atom_dict['calculation_atom_type'] == CalculationAtomType.CREATE else False
            )

    @staticmethod
    def get_calc_atom_object_and_type(calculation_atom_name, model_id):
        """
        获取统计口径object(DmmModelCalculationAtom object/DmmModelCalculationAtomImage object) & 统计口径类型
        :param calculation_atom_name: {String} 统计口径名称
        :param model_id: {Int} 模型ID
        :return:calc_atom_obj: {Object} 统计口径object, calculation_atom_type: {String} 统计口径类型:create/quote
        """
        is_calculation_atom_created, calculation_atom_queryset = is_objects_existed(
            DmmModelCalculationAtom, model_id=model_id, calculation_atom_name=calculation_atom_name
        )
        if is_calculation_atom_created:
            # 统计口径在当前模型下是创建
            calc_atom_obj = calculation_atom_queryset[0]
            calculation_atom_type = CalculationAtomType.CREATE
        else:
            calc_atom_obj = get_object(
                DmmModelCalculationAtomImageStage, model_id=model_id, calculation_atom_name=calculation_atom_name
            )
            calculation_atom_type = CalculationAtomType.QUOTE
        return calc_atom_obj, calculation_atom_type

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def delete_calculation_atom(cls, calculation_atom_name, model_id, bk_username):
        """
        删除统计口径
        :param calculation_atom_name: {String} 统计口径名称
        :param model_id: {Int} 模型ID
        :param bk_username: {String} 用户名
        :return:
        """
        # 1) 判断统计口径在当前模型下是创建还是引用
        calculation_atom_object, calculation_atom_type = cls.get_calc_atom_object_and_type(
            calculation_atom_name, model_id
        )

        # 2) 判断是否可以删除统计口径记录
        if not cls.is_calatom_deletable_param_editable(model_id, [calculation_atom_name])[calculation_atom_name][
            'deletable'
        ]:
            raise dm_pro_errors.CalculationAtomCanNotBeDeletedError(message=_('被指标应用/被其他模型引用的统计口径不能被删除'))

        # 3) 获取变更前的统计口径详情
        orig_calculation_atom_dict = cls.get_calculation_atom_info(calculation_atom_name)

        # 4) 删除统计口径记录
        calculation_atom_object.delete()

        # 5) 获取模型基本信息object
        datamodel_object = get_object(DmmModelInfo, model_id=model_id)

        # 6) 更新模型publish_status状态
        step_id, publish_status = update_data_model_state(
            datamodel_object.model_id, cls.delete_calculation_atom.__name__, bk_username
        )

        # 7) 增加操作记录
        OperationLogManager.create_operation_log(
            created_by=bk_username,
            model_id=model_id,
            object_type=DataModelObjectType.CALCULATION_ATOM.value,
            object_operation=DataModelObjectOperationType.DELETE.value,
            object_id=calculation_atom_name,
            object_name=calculation_atom_name,
            object_alias=orig_calculation_atom_dict['calculation_atom_alias'],
            content_before_change=orig_calculation_atom_dict,
        )
        return {'step_id': step_id, 'publish_status': publish_status}

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def delete_all_calc_atoms_in_model(cls, model_id):
        """
        删除模型中所有的统计口径（模型中创建的统计口径 & 模型中应用的统计口径）
        :param model_id: {Int} 模型ID
        :return:
        """
        # 由于元数据同步不支持queryset的删除，只能在for循环中删除
        # 1) 删除模型中创建的统计口径
        calculation_atom_queryset = DmmModelCalculationAtom.objects.filter(model_id=model_id)
        for calculation_atom_obj in calculation_atom_queryset:
            calculation_atom_obj.delete()

        # 2) 删除模型中引用的统计口径
        calculation_atom_image_queryset = DmmModelCalculationAtomImage.objects.filter(model_id=model_id)
        for calculation_atom_image_obj in calculation_atom_image_queryset:
            calculation_atom_image_obj.delete()
        calculation_atom_image_stage_queryset = DmmModelCalculationAtomImageStage.objects.filter(model_id=model_id)
        for calculation_atom_image_obj in calculation_atom_image_stage_queryset:
            calculation_atom_image_obj.delete()

    @staticmethod
    def validate_calc_atoms_existed(calculation_atom_names):
        """
        批量判断被引用的统计口径列表是否存在, 存在返回引用的统计口径queryset
        :param calculation_atom_names: {List} 统计口径名称列表
        :return: calculation_atom_queryset: {Queryset} 统计口径queryset
        """
        calculation_atom_queryset = DmmModelCalculationAtom.objects.filter(
            calculation_atom_name__in=calculation_atom_names
        )
        orig_calculation_atom_names = [
            calculation_atom_obj.calculation_atom_name for calculation_atom_obj in calculation_atom_queryset
        ]

        add_calculation_atom_names, _, _ = list_diff(orig_calculation_atom_names, calculation_atom_names)
        if add_calculation_atom_names:
            raise dm_pro_errors.CalculationAtomNotExistError()
        return calculation_atom_queryset

    @staticmethod
    def validate_calculation_atoms_quoted(model_id, calculation_atom_names, calculation_atom_queryset):
        """
        校验当前模型是否能引用统计口径列表,返回dmm_model_calculation_atom_image需要新引用的统计口径名称列表
        :param model_id: {Int} 模型ID
        :param calculation_atom_names: {List} 引用统计口径名称列表
        :param calculation_atom_queryset: {QuerySet} 待引用的统计口径queryset
        :return: add_quoted_calc_atom_names: {List} 需要新引用的统计口径名称列表
        """
        # 1) 校验统计口径是否是该模型中创建的统计口径
        for calc_atom_obj in calculation_atom_queryset:
            if calc_atom_obj.model_id == model_id:
                raise dm_pro_errors.CalculationAtomCanNotBeQuotedError()
        # 2) 校验统计口径是否已经被引用,已经被引用的统计口径不在重复引用
        # 已经被引用的统计口径
        quoted_calculation_atom_queryset = DmmModelCalculationAtomImageStage.objects.filter(
            model_id=model_id, calculation_atom_name__in=calculation_atom_names
        )
        quoted_calculation_atom_names = [
            calculation_atom_obj.calculation_atom_name for calculation_atom_obj in quoted_calculation_atom_queryset
        ]
        # 待新增 & 待删除的统计口径
        added_quoted_calc_atom_names, _, updated_quoted_calc_atom_names = list_diff(
            quoted_calculation_atom_names, calculation_atom_names
        )
        if updated_quoted_calc_atom_names:
            logger.warning('calc_atom_names:{} have already been quoted'.format(updated_quoted_calc_atom_names))
        return added_quoted_calc_atom_names

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def quote_calculation_atoms(cls, params, bk_username):
        """
        草稿态引用统计口径
        :param params: {dict} 引用统计口径参数{'model_id':1, 'calculation_atom_names':[]}
        :param bk_username: {str} 用户名
        :return: {dict}: 模型构建完成步骤 & 发布状态{'step_id':1}
        """
        # 1) 判断模型是否存在
        datamodel_object = get_object(DmmModelInfo, model_id=params['model_id'])

        # 2) 批量判断被引用的统计口径是否存在
        calculation_atom_queryset = cls.validate_calc_atoms_existed(params['calculation_atom_names'])

        # 3) 判断模型是否能引用 & 是否已经引用统计口径列表,返回dmm_model_calculation_atom_image需要新引用的统计口径名称列表
        add_quoted_calc_atom_names = cls.validate_calculation_atoms_quoted(
            params['model_id'], params['calculation_atom_names'], calculation_atom_queryset
        )

        # 4) 校验引用统计口径的统计方式 & 保存统计口径引用记录
        calc_atom_dict = {}
        for calculation_atom_obj in calculation_atom_queryset:
            if calculation_atom_obj.calculation_atom_name in add_quoted_calc_atom_names:
                calc_atom_dict[calculation_atom_obj.calculation_atom_name] = {
                    'calculation_formula': calculation_atom_obj.calculation_formula,
                    'field_type': calculation_atom_obj.field_type,
                    'calculation_atom_alias': calculation_atom_obj.calculation_atom_alias,
                }

        schema_list = get_schema_list(params['model_id'])
        for calculation_atom_name in add_quoted_calc_atom_names:
            # 校验引用统计口径的统计方式
            validate_calculation_content(
                datamodel_object.table_name,
                calculation_atom_name,
                calc_atom_dict[calculation_atom_name]['field_type'],
                calc_atom_dict[calculation_atom_name]['calculation_formula'],
                schema_list,
            )
            # 新增引用统计口径记录
            calculation_atom_image_object = DmmModelCalculationAtomImageStage(
                model_id=params['model_id'],
                project_id=datamodel_object.project_id,
                calculation_atom_name=calculation_atom_name,
                created_by=bk_username,
                updated_by=bk_username,
            )
            calculation_atom_image_object.save()

            # 5) 操作记录
            OperationLogManager.create_operation_log(
                created_by=bk_username,
                model_id=params['model_id'],
                object_type=DataModelObjectType.CALCULATION_ATOM.value,
                object_operation=DataModelObjectOperationType.CREATE.value,
                object_id=calculation_atom_name,
                object_name=calculation_atom_name,
                object_alias=calc_atom_dict[calculation_atom_name]['calculation_atom_alias'],
                content_after_change=cls.get_calculation_atom_info(calculation_atom_name),
            )

        # 6) 更新模型publish_status状态
        _, publish_status = update_data_model_state(
            params['model_id'], cls.quote_calculation_atoms.__name__, bk_username
        )

        # 7) 返回模型构建&发布步骤
        return {'step_id': datamodel_object.step_id, 'publish_status': publish_status}

    @classmethod
    def sync_calatom_image_from_stage(cls, model_id, calculation_atom_image_list, bk_username):
        """
        模型发布，将草稿态统计口径引用同步到db主表
        :param model_id: {int} 模型ID
        :param calculation_atom_image_list: {list} 草稿态统计口径引用列表
        :param bk_username: {str} 用户名
        :return:
        """
        update_data_models_objects(
            model_id, DmmModelCalculationAtomImage, calculation_atom_image_list, bk_username, ['calculation_atom_name']
        )

    @staticmethod
    def get_calatom_can_be_quoted_in_model(model_id, field_names):
        """
        获取模型可以引用的统计口径列表
        :param model_id: {Int} 需要排除统计口径的模型ID
        :param field_names: {List} 模型主表字段名称列表
        :return: calc_atom_list: {List} 模型可以引用的统计口径列表
        """
        # 1）数据集市所有的统计口径
        calc_atom_queryset = DmmModelCalculationAtom.objects.all().order_by('-updated_at')

        # 2）模型已经引用的统计口径
        exclude_calculation_atom_names = [
            exclude_calc_atom_obj.calculation_atom_name
            for exclude_calc_atom_obj in DmmModelCalculationAtomImageStage.objects.filter(model_id=model_id)
        ]

        # 3）排除模型的统计口径和模型已经引用的统计口径
        calc_atom_queryset = calc_atom_queryset.exclude(model_id=model_id).exclude(
            calculation_atom_name__in=exclude_calculation_atom_names
        )

        # 4）获取加工来源字段在主表中的统计口径
        calc_atom_list = [
            models_object_to_dict(
                cal_atom_obj,
                type=DataModelObjectType.CALCULATION_ATOM.value,
                calculation_atom_type=CalculationAtomType.CREATE,
            )
            for cal_atom_obj in calc_atom_queryset
            if set(cal_atom_obj.origin_fields).issubset(field_names)
        ]
        return calc_atom_list

    @staticmethod
    def get_quoted_cal_atoms_in_model(model_id):
        """
        获取模型中引用的统计口径
        :param model_id: {int} 模型ID
        :return: quoted_calc_atom_list {list} 引用的统计口径列表
        """
        quoted_calc_atom_names = [
            calc_atom_dict['calculation_atom_name']
            for calc_atom_dict in DmmModelCalculationAtomImageStage.objects.filter(model_id=model_id)
            .order_by('-updated_at')
            .values('calculation_atom_name')
        ]
        quoted_calc_atom_list = [
            models_object_to_dict(
                calc_atom_obj,
                type=DataModelObjectType.CALCULATION_ATOM.value,  # type用于前端知道树形结构点击的节点类型
                calculation_atom_type=CalculationAtomType.QUOTE,
                calculation_formula_stripped_comment=strip_comment(calc_atom_obj.calculation_formula),
            )
            for calc_atom_obj in DmmModelCalculationAtom.objects.filter(
                calculation_atom_name__in=quoted_calc_atom_names
            )
        ]
        return quoted_calc_atom_list

    @classmethod
    def get_calculation_atom_object_list(cls, model_id):
        """
        获取模型创建和引用的统计口径object列表
        :param model_id: {int} 模型ID
        :return: {[object]} 模型创建和引用的统计口径object列表
        """
        calc_atom_image_queryset = DmmModelCalculationAtomImageStage.objects.filter(model_id=model_id)
        calc_atom_names = [
            calc_atom_image_obj.calculation_atom_name for calc_atom_image_obj in calc_atom_image_queryset
        ]
        calc_atom_queryset = DmmModelCalculationAtom.objects.filter(
            Q(model_id=model_id) | Q(calculation_atom_name__in=calc_atom_names)
        )
        return list(calc_atom_queryset)

    @classmethod
    def get_calculation_atom_list(cls, params):
        """
        获取统计口径列表
        :param params: {Dict} 获取统计口径列表接口参数 {'model_id': 3, 'with_indicators': True}
        :return:calc_atom_list {List} 统计口径列表, step_id {Int} 模型构建&发布步骤, publish_status {String} 模型发布状态
        """
        with_indicators = params.get('with_indicators', False)
        with_details = params.get('with_details', [])

        # 1) 模型创建的统计口径信息
        calc_atom_list = [
            models_object_to_dict(
                cal_atom_obj,
                type=DataModelObjectType.CALCULATION_ATOM.value,
                calculation_atom_type=CalculationAtomType.CREATE,
                calculation_formula_stripped_comment=strip_comment(cal_atom_obj.calculation_formula),
            )
            for cal_atom_obj in DmmModelCalculationAtom.objects.filter(model_id=params['model_id']).order_by(
                '-updated_at'
            )
        ]

        # 2) 模型中引用的统计口径
        quoted_calc_atom_list = cls.get_quoted_cal_atoms_in_model(params['model_id'])
        calc_atom_list.extend(quoted_calc_atom_list)

        calc_atom_names = [calc_atom_dict['calculation_atom_name'] for calc_atom_dict in calc_atom_list]
        # 统计口径列表是否可以被删除 & 关键参数是否可编辑:是否被其他模型引用 / 被指标应用
        cls.set_calatom_deletable_editable(params['model_id'], calc_atom_names, calc_atom_list)
        # 统计口径在S3页面的统计口径是否禁用(模型下引用的统计口径编辑按钮禁用)
        cls.set_calculation_atoms_editable(calc_atom_list)
        # 指标数量
        cls.set_indicator_count(params['model_id'], calc_atom_names, calc_atom_list)
        # 是否返回指标信息
        if with_indicators:
            cls.set_indicators(params['model_id'], calc_atom_list)

        if 'fields' in with_details:
            # 用于模型预览展示统计口径节点聚合逻辑
            for calc_atom_dict in calc_atom_list:
                calc_atom_dict['fields'] = [{'field_name': strip_comment(calc_atom_dict['calculation_formula'])}]

        # 3) 模型构建&发布完成步骤
        datamodel_object = get_object(DmmModelInfo, model_id=params['model_id'])
        step_id = datamodel_object.step_id
        publish_status = datamodel_object.publish_status
        return calc_atom_list, step_id, publish_status

    @classmethod
    def get_cal_atom_list_can_be_quoted(cls, model_id):
        """
        获取当前模型可以引用的统计口径列表
        :param model_id: {int} 模型ID
        :return: {list} 可以被引用的统计口径列表
        """
        # 当前模型主表字段名称列表,用于剔除加工字段不在主表中的统计口径
        master_table_field_names = [
            field_obj.field_name for field_obj in DmmModelFieldStage.objects.filter(model_id=model_id)
        ]
        calc_atom_list = cls.get_calatom_can_be_quoted_in_model(model_id, master_table_field_names)
        calc_atom_names = [calc_atom_dict['calculation_atom_name'] for calc_atom_dict in calc_atom_list]
        # 引用数量
        cls.set_quoted_count(calc_atom_names, calc_atom_list)
        return calc_atom_list

    @staticmethod
    def set_indicators(model_id, calc_atom_list):
        """
        返回指标信息
        :param model_id: {Int} 模型ID
        :param calc_atom_list: {List} 统计口径列表
        :return:
        """
        indicators, _, _ = IndicatorManager.get_indicator_list(
            {'model_id': model_id, 'with_sub_indicators': True}, type=DataModelObjectType.INDICATOR.value
        )
        calc_atom_indicator_dict = {}
        for indicator_dict in indicators:
            if indicator_dict['calculation_atom_name'] not in calc_atom_indicator_dict:
                calc_atom_indicator_dict[indicator_dict['calculation_atom_name']] = []
            calc_atom_indicator_dict[indicator_dict['calculation_atom_name']].append(indicator_dict)
        for calc_atom_dict in calc_atom_list:
            if calc_atom_dict['calculation_atom_name'] in calc_atom_indicator_dict:
                calc_atom_dict['indicators'] = calc_atom_indicator_dict[calc_atom_dict['calculation_atom_name']]

    @staticmethod
    def get_quoted_count(calculation_atom_names):
        """
        获取统计口径被引用数量
        :param calculation_atom_names: {List}, 统计口径名称列表
        :return: {Dict} 统计口径被引用数量
        """
        calc_atom_quoted_count_queryset = (
            DmmModelCalculationAtomImage.objects.filter(calculation_atom_name=calculation_atom_names)
            .values('calculation_atom_name')
            .annotate(quoted_count=Count('calculation_atom_name'))
        )
        calc_atom_quoted_count_dict = {
            count_dict['calculation_atom_name']: count_dict['quoted_count']
            for count_dict in calc_atom_quoted_count_queryset
        }
        return calc_atom_quoted_count_dict

    @classmethod
    def set_quoted_count(cls, calculation_atom_names, calc_atom_list):
        """
        设置统计口径引用数量
        :param calculation_atom_names: {List} 统计口径名称列表
        :param calc_atom_list: {List} 统计口径列表
        :return:
        """
        calc_atom_quoted_count_dict = cls.get_quoted_count(calculation_atom_names)
        for calc_atom_dict in calc_atom_list:
            calc_atom_dict['quoted_count'] = calc_atom_quoted_count_dict.get(calc_atom_dict['calculation_atom_name'], 0)

    @staticmethod
    def get_indicator_count(model_id, calculation_atom_names):
        """
        返回统计口径下面的当前模型指标数量
        :param model_id: {Int}, 模型ID
        :param calculation_atom_names: {List}, 统计口径名称列表
        :return: {Dict} 统计口径指标数量
        """
        calc_atom_indicator_count_queryset = (
            DmmModelIndicatorStage.objects.filter(model_id=model_id, calculation_atom_name__in=calculation_atom_names)
            .values('calculation_atom_name')
            .annotate(indicator_count=Count('calculation_atom_name'))
        )
        calc_atom_indicator_count_dict = {
            count_dict['calculation_atom_name']: count_dict['indicator_count']
            for count_dict in calc_atom_indicator_count_queryset
        }
        return calc_atom_indicator_count_dict

    @classmethod
    def set_indicator_count(cls, model_id, calculation_atom_names, calc_atom_list):
        """
        设置指标数量indicator_count
        :param model_id: {int} 模型ID
        :param calculation_atom_names: {list}, 统计口径名称列表
        :param calc_atom_list: {list}, 统计口径列表
        :return:
        """
        calc_atom_indicator_count_dict = cls.get_indicator_count(model_id, calculation_atom_names)
        for calc_atom_dict in calc_atom_list:
            calc_atom_dict['indicator_count'] = calc_atom_indicator_count_dict.get(
                calc_atom_dict['calculation_atom_name'], 0
            )

    @classmethod
    def get_calculation_atom_info(cls, calculation_atom_name, with_indicators=False):
        """
        获取统计口径信息
        :param calculation_atom_name: {String} 统计口径名称
        :param with_indicators: {Boolean} 是否返回指标信息,默认为False
        :return: calculation_atom_dict: {Dict} 统计口径信息
        """
        # 1) 判断统计口径是否存在
        calculation_atom_object = get_object(DmmModelCalculationAtom, calculation_atom_name=calculation_atom_name)

        # 2) 统计口径信息
        calculation_atom_dict = models_object_to_dict(
            calculation_atom_object,
            calculation_formula_stripped_comment=strip_comment(calculation_atom_object.calculation_formula),
        )

        # 3) 模型构建&发布完成步骤
        datamodel_object = get_object(DmmModelInfo, model_id=calculation_atom_object.model_id)
        calculation_atom_dict['step_id'] = datamodel_object.step_id

        # 4) 统计口径是否可以被删除、关键参数是否可以修改
        cls.set_calatom_deletable_editable(datamodel_object.model_id, [calculation_atom_name], [calculation_atom_dict])

        # 5) 是否返回指标信息
        if with_indicators:
            indicators, _, _ = IndicatorManager.get_indicator_list(
                {
                    'model_id': datamodel_object.model_id,
                    'calculation_atom_name': calculation_atom_name,
                    'with_sub_indicators': True,
                }
            )
            calculation_atom_dict['indicators'] = indicators
        return calculation_atom_dict

    @classmethod
    def get_calculation_functions(cls):
        """
        聚合函数列表
        :return: calc_functions: {List} 聚合函数列表
        """
        calc_functions = DmmCalculationFunctionConfig.objects.all().values(
            'function_name', 'output_type', 'allow_field_type'
        )
        return calc_functions
