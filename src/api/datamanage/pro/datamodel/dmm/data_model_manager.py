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


from copy import deepcopy

from django.forms.models import model_to_dict
from django.db.models import Q
from django.db.models import Count
from django.utils.translation import ugettext as _

from common.transaction import auto_meta_sync
from common.log import logger
from common.exceptions import ApiRequestError, ApiResultError

from datamanage.utils.random import get_random_string
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.datamodel import (
    DmmModelInfo,
    DmmModelTop,
    DmmModelRelation,
    DmmModelRelationStage,
    DmmModelRelease,
    DmmModelField,
)
from datamanage.pro.datamodel.models.indicator import (
    DmmModelCalculationAtom,
    DmmModelCalculationAtomImage,
    DmmModelCalculationAtomImageStage,
    DmmModelIndicatorStage,
)
from datamanage.pro.datamodel.models.application import DmmModelInstance
from datamanage.pro.datamodel.models.model_dict import (
    DataModelActiveStatus,
    DataModelType,
    DataModelObjectType,
    DataModelObjectOperationType,
    DataModelStep,
    DataModelUsedInfoDict,
    DataModelPublishStatus,
    CalculationAtomType,
    TimeField,
)
from datamanage.pro.datamodel.dmm.object import get_object, check_and_assign_update_content, models_object_to_dict
from datamanage.pro.datamodel.dmm.master_table_manager import MasterTableManager
from datamanage.pro.datamodel.dmm.calculation_atom_manager import CalculationAtomManager
from datamanage.pro.datamodel.dmm.indicator_manager import IndicatorManager
from datamanage.pro.datamodel.dmm.data_model_diff import (
    get_diff_objects_and_diff_result,
    get_data_model_object_content_list,
)
from datamanage.pro.datamodel.dmm.operation_log_manager import OperationLogManager
from datamanage.pro.datamodel.dmm.schema import get_schema_list
from datamanage.pro.datamodel.dmm.state_machine import update_data_model_state
from datamanage.pro.datamodel.handlers.time import set_created_at_and_updated_at
from datamanage.pro.datamodel.handlers.tag import (
    tag_target,
    update_datamodel_tag,
    get_datamodel_tags,
    get_datamodels_by_tag,
    delete_model_tag,
)
from datamanage.pro.datamodel.handlers.verifier import validate_calculation_content, strip_comment

MAX_TOP_NUM = 20
DELETED_MODEL_NAME_RANDOM_LEN = 4


class DataModelManager(object):
    @staticmethod
    def tag_model_target(tags, datamodel_object, bk_username):
        """
        模型创建时给模型打标签(用户自定义标签 & 系统内置标签)，如果打标签失败，则模型记录回滚
        :param tags: {List} 标签列表 [{'tag_code':'xx', 'tag_alias': 'yy'}]
        :param datamodel_object: {Object} DataModelInfo object
        :param bk_username: {String} 用户名
        :return:
        """
        try:
            tag_target_list = tag_target(datamodel_object.model_id, 'data_model', tags, bk_username)
        except (ApiRequestError, ApiResultError) as e:
            logger.error(
                "tag_datamodel_target request error:{}, params:{}".format(
                    str(e), {'model_id': datamodel_object.model_id, 'tags': tags, 'bk_username': bk_username}
                )
            )
            return None
        tag_target_ret_list = [
            {'tag_alias': tag_target_dict.get('alias', None), 'tag_code': tag_target_dict.get('code', None)}
            for tag_target_dict in tag_target_list
        ]
        return tag_target_ret_list

    @classmethod
    def validate_model_name(cls, params):
        """
        校验模型名称是否重复
        :param params: {Dict} 模型创建参数
        :return:
        """
        datamodel_object = DmmModelInfo.objects.filter(model_name=params['model_name'])
        if datamodel_object.exists():
            logger.warning('data model name already existed when creating data model, params:{}'.format(params))
            return True
        return False

    @staticmethod
    def data_model_object_to_dict(datamodel_object, tags):
        """
        将data_model_object转换为data_model字典，并加上标签属性
        :param datamodel_object: {Object} datamodel_object
        :param tags: {List} 标签列表
        :return: datamodel_dict: {Dict} data_model字典
        """
        datamodel_dict = model_to_dict(datamodel_object)
        # 创建时间 & 修改时间
        set_created_at_and_updated_at(datamodel_dict, datamodel_object)
        # 标签
        datamodel_dict['tags'] = tags
        return datamodel_dict

    @classmethod
    def create_data_model(cls, params, bk_username):
        """
        创建数据模型
        :param params: {Dict} 模型创建参数, 例如
        {
            "model_name": "fact_item_flow",
            "model_alias": "道具流水表",
            "model_type": "fact_table",
            "description": "道具流水",
            "tags": [
                {
                    "tag_code":"common_dimension",
                    "tag_alias":"公共维度"
                },{
                    "tag_code":"",
                    "tag_alias":"自定义标签名称"
                }
            ],
            "project_id": 1
        }
        :param bk_username: {String} 用户名
        :return: datamodel_dict: {Dict} 数据模型信息
        """
        # 1) 判断model_name对应的数据模型是否存在
        is_model_name_existed = cls.validate_model_name(params)
        if is_model_name_existed:
            raise dm_pro_errors.DataModelNameAlreadyExistError

        # 2）新增数据模型记录,将完成步骤设置为1,将发布状态设置为developing
        with auto_meta_sync(using='bkdata_basic'):
            datamodel_object = DmmModelInfo(
                model_name=params['model_name'],
                model_alias=params['model_alias'],
                table_name=params['model_name'],
                table_alias=params['model_alias'],
                model_type=params['model_type'],
                project_id=params['project_id'],
                description=params['description'],
                created_by=bk_username,
                updated_by=bk_username,
            )
            datamodel_object.save()

        # 更新模型step_id状态
        step_id, _ = update_data_model_state(datamodel_object.model_id, cls.create_data_model.__name__, bk_username)

        # 3) 给数据模型打标签（自定义标签 & 自定义标签）
        tags = params['tags']
        ret_tags = cls.tag_model_target(tags, datamodel_object, bk_username)
        if not ret_tags:
            # 打标签失败对模型记录做回滚
            datamodel_object.delete()
            raise dm_pro_errors.DataModelTAGTARGETError()

        # 4）返回数据模型信息
        datamodel_dict = models_object_to_dict(datamodel_object, tags=ret_tags, step_id=step_id)

        # 5) 增加操作记录
        OperationLogManager.create_operation_log(
            created_by=bk_username,
            model_id=datamodel_dict['model_id'],
            object_type=DataModelObjectType.MODEL.value,
            object_operation=DataModelObjectOperationType.CREATE.value,
            object_id=datamodel_dict['model_id'],
            object_name=datamodel_dict['model_name'],
            object_alias=datamodel_dict['model_alias'],
            content_after_change=datamodel_dict,
        )
        return datamodel_dict

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def update_data_model(cls, model_id, params, bk_username):
        """
        修改数据模型
        :param model_id: {Int} 模型ID
        :param params: {Dict} 模型修改参数
        :param bk_username: {String} 用户名
        :return: datamodel_dict: {Dict} 修改后的数据模型信息
        """
        # 1) 判断model_id对应的数据模型是否存在
        datamodel_object = get_object(DmmModelInfo, model_id=model_id)
        publish_status = datamodel_object.publish_status

        # 获取模型修改前的模型详情
        orig_tags = get_datamodel_tags([model_id], with_tag_alias=True).get(model_id, [])
        # 将datamodel_object转化为datamodel_dict
        orig_datamodel_dict = models_object_to_dict(datamodel_object, tags=orig_tags, publish_status=publish_status)

        # 2) 修改数据模型记录
        # 判断模型是否有需要修改的内容
        has_update = check_and_assign_update_content(datamodel_object, params, allow_null_fields=['description'])
        # 有需要修改的内容
        if has_update:
            # 主表别名和模型别名一致
            datamodel_object.table_alias = datamodel_object.model_alias
            datamodel_object.updated_by = bk_username
            datamodel_object.save()

            # 更新模型publish_status状态
            _, publish_status = update_data_model_state(model_id, cls.update_data_model.__name__, bk_username)

        # 3) 修改模型标签
        has_tags_updated = False
        if 'tags' in params:
            has_tags_updated = update_datamodel_tag(model_id, params['tags'], bk_username)

        # 4）返回修改后数据模型信息
        # 模型标签
        tag_target_dict = get_datamodel_tags([model_id], with_tag_alias=True)
        tags = tag_target_dict.get(model_id, [])

        # 将datamodel_object转化为datamodel_dict
        datamodel_dict = models_object_to_dict(datamodel_object, tags=tags, publish_status=publish_status)

        # 5) 有关键参数更新的时候，才添加操作记录
        if has_update or has_tags_updated:
            OperationLogManager.create_operation_log(
                created_by=bk_username,
                model_id=datamodel_dict['model_id'],
                object_type=DataModelObjectType.MODEL.value,
                object_operation=DataModelObjectOperationType.UPDATE.value,
                object_id=datamodel_dict['model_id'],
                object_name=datamodel_dict['model_name'],
                object_alias=datamodel_dict['model_alias'],
                content_before_change=orig_datamodel_dict,
                content_after_change=datamodel_dict,
            )
        return datamodel_dict

    @classmethod
    def get_data_model_info(cls, model_id, params):
        """
        数据模型详情
        :param model_id: {int} 模型ID
        :param params: {dict} 参数字典,例如{'with_details':['master_table']}
        :return: datamodel_dict: {Dict}数据模型信息
        """
        # 1) 判断model_id对应的数据模型是否存在
        datamodel_object = get_object(DmmModelInfo, model_id=model_id)

        # 2）返回数据模型信息
        # 模型对应标签
        tags = get_datamodel_tags([model_id], with_tag_alias=True).get(model_id, [])
        # 将datamodel_object转化为datamodel_dict
        datamodel_dict = models_object_to_dict(datamodel_object, exclude_fields=['latest_version_id'], tags=tags)

        # 3) 展示模型主表字段、模型关联关系、统计口径、指标等详情
        if 'with_details' in params:
            datamodel_dict['model_detail'] = {}
            if 'release_info' in params['with_details']:
                if datamodel_object.latest_version_id is not None:
                    datamodel_release_queryset = DmmModelRelease.objects.filter(
                        version_id=datamodel_object.latest_version_id
                    )
                    if datamodel_release_queryset.exists():
                        datamodel_dict['version_log'] = datamodel_release_queryset[0].version_log
                        datamodel_dict['release_created_by'] = datamodel_release_queryset[0].created_by
                        datamodel_dict['release_created_at'] = datamodel_release_queryset[0].created_at

            if 'master_table' in params['with_details']:
                # 主表信息 & 关联关系
                master_table_dict = MasterTableManager.get_master_table_info(model_id, with_time_field=True)
                datamodel_dict['model_detail'].update(
                    {
                        'fields': master_table_dict['fields'],
                        'model_relation': master_table_dict['model_relation'],
                    }
                )
            if 'calculation_atoms' in params['with_details']:
                # 统计口径列表
                calculation_atoms_list, _, _ = CalculationAtomManager.get_calculation_atom_list({'model_id': model_id})
                datamodel_dict['model_detail']['calculation_atoms'] = calculation_atoms_list
            if 'indicators' in params['with_details']:
                # 指标列表
                indicator_list, _, _ = IndicatorManager.get_indicator_list({'model_id': model_id})
                datamodel_dict['model_detail']['indicators'] = indicator_list
        return datamodel_dict

    @classmethod
    def get_data_model_latest_version_info(cls, model_id, with_details=None):
        """获取数据模型最新发布版本内容（查看态详情内容）

        :param model_id: 模型ID
        :param with_details: 返回统计口径 & 指标在草稿态中是否存在
        :type with_details: list
        :return: 数据模型最新发布版本内容，格式如:

            {'model_detail': {'indicators': [], 'calculation_atom_name': []}}

        """
        # 1) 获取数据模型最新发布版本内容(模型基本信息替换为最新状态)
        datamodel_content_dict = cls.get_model_latest_ver_content_dict(model_id)
        # 模型尚未发布
        if not datamodel_content_dict:
            return {}
        if 'model_detail' not in datamodel_content_dict:
            raise dm_pro_errors.DataModelReleaseHasNoModelContentError()

        # 2) 统计口径 & 指标
        if with_details is None:
            with_details = []
        # 统计口径 & 指标在草稿态中是否存在
        if 'existed_in_stage' in with_details:
            cls.set_calatom_ind_existed_in_stage(model_id, datamodel_content_dict['model_detail'])

        # a) 获取指标树形结构
        indicators = IndicatorManager.get_indicator_tree(datamodel_content_dict['model_detail'].get('indicators', []))
        # b) 构建统计口径和指标树形结构（将指标放入对应的统计口径下面）
        calc_atom_indicator_dict = {}
        for indicator_dict in indicators:
            if indicator_dict['calculation_atom_name'] not in calc_atom_indicator_dict:
                calc_atom_indicator_dict[indicator_dict['calculation_atom_name']] = []
            calc_atom_indicator_dict[indicator_dict['calculation_atom_name']].append(indicator_dict)
        for calc_atom_dict in datamodel_content_dict['model_detail'].get('calculation_atoms', []):
            if calc_atom_dict['calculation_atom_name'] in calc_atom_indicator_dict:
                calc_atom_dict['indicators'] = calc_atom_indicator_dict[calc_atom_dict['calculation_atom_name']]
        return datamodel_content_dict

    @staticmethod
    def set_calatom_ind_existed_in_stage(model_id, data_model_detail_dict):
        """设置最新发布态的统计口径和指标在模型草稿态是否存在

        :param model_id: 模型ID
        :param data_model_detail_dict: 模型最新发布内容详情

        :return:
        """
        indicator_names = [
            indicator_dict['indicator_name'] for indicator_dict in data_model_detail_dict.get('indicators', [])
        ]
        indicator_names_in_stage = list(
            DmmModelIndicatorStage.objects.filter(model_id=model_id, indicator_name__in=indicator_names)
            .values('indicator_name')
            .annotate(count=Count('indicator_name'))
            .values_list('indicator_name', flat=True)
        )
        for indicator_dict in data_model_detail_dict.get('indicators', []):
            indicator_dict['existed_in_stage'] = True
            if indicator_dict['indicator_name'] not in indicator_names_in_stage:
                indicator_dict['existed_in_stage'] = False

        # 模型中引用的统计口径
        quoted_calc_atom_names = [
            calc_atom_dict['calculation_atom_name']
            for calc_atom_dict in data_model_detail_dict.get('calculation_atoms', [])
            if calc_atom_dict['calculation_atom_type'] == CalculationAtomType.QUOTE
        ]
        # 草稿态中引用的统计口径
        quoted_calculation_atom_names_in_stage = list(
            DmmModelCalculationAtomImageStage.objects.filter(
                model_id=model_id, calculation_atom_name__in=quoted_calc_atom_names
            )
            .values('calculation_atom_name')
            .annotate(count=Count('calculation_atom_name'))
            .values_list('calculation_atom_name', flat=True)
        )

        # 模型中创建的统计口径
        calc_atom_names = [
            calc_atom_dict['calculation_atom_name']
            for calc_atom_dict in data_model_detail_dict.get('calculation_atoms', [])
            if calc_atom_dict['calculation_atom_type'] == CalculationAtomType.CREATE
        ]
        calculation_atom_names_in_stage = list(
            DmmModelCalculationAtom.objects.filter(model_id=model_id, calculation_atom_name__in=calc_atom_names)
            .values('calculation_atom_name')
            .annotate(count=Count('calculation_atom_name'))
            .values_list('calculation_atom_name', flat=True)
        )

        for calc_atom_dict in data_model_detail_dict.get('calculation_atoms', []):
            calc_atom_dict['existed_in_stage'] = True
            if (
                calc_atom_dict['calculation_atom_type'] == CalculationAtomType.QUOTE
                and calc_atom_dict['calculation_atom_name'] not in quoted_calculation_atom_names_in_stage
            ):
                calc_atom_dict['existed_in_stage'] = False
            elif (
                calc_atom_dict['calculation_atom_type'] == CalculationAtomType.CREATE
                and calc_atom_dict['calculation_atom_name'] not in calculation_atom_names_in_stage
            ):
                calc_atom_dict['existed_in_stage'] = False

    @staticmethod
    def get_model_latest_ver_content_dict(model_id, version_id=None):
        """
        获取数据模型最新/指定发布版本内容(模型基本信息替换为最新状态)
        :param model_id: {int} 模型ID
        :param version_id: {str} 版本id
        :return: datamodel_content_dict {dict}:数据模型最新发布版本内容
        """
        # 1) 判断模型是否存在
        datamodel_object = get_object(DmmModelInfo, model_id=model_id)
        # 模型未发布
        if datamodel_object.latest_version_id is None:
            return {}

        # 2) 获取数据模型最新发布版本内容
        if version_id is None:
            version_id = datamodel_object.latest_version_id
        datamodel_release_obj = get_object(DmmModelRelease, version_id=version_id)
        datamodel_content_dict = datamodel_release_obj.model_content
        if not datamodel_content_dict:
            raise dm_pro_errors.DataModelReleaseHasNoModelContentError()

        # 3) 补充模型最新发布版本信息
        datamodel_content_dict['version_log'] = datamodel_release_obj.version_log
        datamodel_content_dict['release_created_by'] = datamodel_release_obj.created_by
        datamodel_content_dict['release_created_at'] = datamodel_release_obj.created_at

        # 4) 将模型基本信息替换为最新状态
        datamodel_content_dict['model_alias'] = datamodel_object.model_alias
        datamodel_content_dict['description'] = datamodel_object.description
        datamodel_content_dict['tags'] = get_datamodel_tags([model_id], with_tag_alias=True).get(model_id, [])
        return datamodel_content_dict

    @staticmethod
    def validate_model_content(datamodel_content_dict):
        """
        在模型发布前，对模型内容进行校验
        :param datamodel_content_dict: {Dict} 模型相关内容
        :return:
        """
        schema_list = get_schema_list(datamodel_content_dict['model_id'])
        # 1）对模型中创建和引用的统计口径进行校验
        calcaltion_atom_dict = {}
        for calc_atom_dict in datamodel_content_dict['model_detail']['calculation_atoms']:
            calcaltion_atom_dict[calc_atom_dict['calculation_atom_name']] = calc_atom_dict
            try:
                validate_calculation_content(
                    datamodel_content_dict['table_name'],
                    calc_atom_dict['calculation_atom_name'],
                    calc_atom_dict['field_type'],
                    calc_atom_dict['calculation_formula'],
                    schema_list,
                )
            except dm_pro_errors.SqlValidateError as e:
                logger.error(
                    'model content validation failed for:{}, calcaltion_atom_dict:{}'.format(
                        str(e), calcaltion_atom_dict
                    )
                )
                raise dm_pro_errors.ModelReleaseContentValidationError(
                    message_kv={'error_info': '统计口径{} {}'.format(calc_atom_dict['calculation_atom_name'], str(e))}
                )

        # 2）对模型中的指标进行校验
        for indicator_dict in datamodel_content_dict['model_detail']['indicators']:
            filter_formula = strip_comment(indicator_dict['filter_formula'])
            # 输入字段列表：当前指标为衍生指标,输入字段为父指标字段;当前指标非衍生指标,输入字段为主表字段
            indicator_input_schema_list = (
                IndicatorManager.get_indicator_fields(
                    indicator_dict['parent_indicator_name'], indicator_dict['model_id'], with_time_field=False
                )
                if indicator_dict['parent_indicator_name']
                else schema_list
            )
            # 校验统计SQl、过滤条件和聚合字段
            try:
                validate_calculation_content(
                    datamodel_content_dict['table_name'],
                    indicator_dict['calculation_atom_name'],
                    calcaltion_atom_dict[indicator_dict['calculation_atom_name']]['field_type'],
                    calcaltion_atom_dict[indicator_dict['calculation_atom_name']]['calculation_formula'],
                    indicator_input_schema_list,
                    filter_formula,
                    indicator_dict['aggregation_fields'],
                )
            except dm_pro_errors.SqlValidateError as e:
                logger.error('model content validation failed for:{}, indicator_dict:{}'.format(str(e), indicator_dict))
                raise dm_pro_errors.ModelReleaseContentValidationError(
                    message_kv={'error_info': '指标{} {}'.format(indicator_dict['indicator_name'], str(e))}
                )

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def release_data_model(cls, model_id, version_log, bk_username, allow_no_diff=False):
        """
        发布数据模型
        :param model_id: {int} 模型ID
        :param version_log: {str} 发布描述
        :param bk_username: {str} 用户名
        :param allow_no_diff: {bool} 是否允许没有diff的模型再次发布
        :return: {Dict} datamodel_release_dict 模型发布记录
        """
        # 1) 判断模型是否存在
        datamodel_object = get_object(DmmModelInfo, model_id=model_id)
        # 获取模型历史发布版本的信息
        latest_datamodel_content_dict = (
            get_object(DmmModelRelease, version_id=datamodel_object.latest_version_id).model_content
            if datamodel_object.latest_version_id is not None
            else {}
        )
        # 获取模型当前内容
        datamodel_content_dict = cls.get_data_model_info(
            model_id, {'with_details': ['master_table', 'calculation_atoms', 'indicators']}
        )

        # 2) 模型发布校验 & 获取关联模型最新版本信息
        related_models_latest_version_id_dict = cls.validate_model_content_in_release(
            model_id, datamodel_content_dict, allow_no_diff
        )

        # 3) 补充被关联维度模型的最新版本id
        for model_relation in datamodel_content_dict['model_detail']['model_relation']:
            if model_relation['related_model_id'] not in related_models_latest_version_id_dict:
                raise dm_pro_errors.DataModelHasNotBeenReleasedError(
                    message=_('被关联的维度模型:{}尚未被发布'.format(model_relation['related_model_id']))
                )
            model_relation['related_model_version_id'] = related_models_latest_version_id_dict[
                model_relation['related_model_id']
            ]

        # 4) 同步草稿表内容到db主表
        cls.sync_model_con_from_stage_to_master(model_id, datamodel_content_dict, bk_username)

        # 5) 更新step_id和publish_status状态
        step_id, publish_status = update_data_model_state(model_id, cls.release_data_model.__name__, bk_username)

        # 发布状态更新
        datamodel_content_dict['publish_status'] = publish_status
        # 模型构建&发布步骤
        datamodel_content_dict['step_id'] = step_id

        # 6) 生成32位字母+数字随机数作为version_id
        version_id = get_random_string(32)
        datamodel_content_dict['latest_version'] = version_id

        # 7) 保存模型发布记录
        datamodel_release_object = DmmModelRelease(
            version_id=version_id,
            version_log=version_log,
            model_id=model_id,
            model_content=datamodel_content_dict,
            created_by=bk_username,
            updated_by=bk_username,
        )
        datamodel_release_object.save()

        # 8) 更新dmm_model_info的version_id
        datamodel_object.latest_version = datamodel_release_object
        datamodel_object.publish_status = publish_status
        datamodel_object.step_id = step_id
        datamodel_object.save()

        # 9) 增加操作记录
        OperationLogManager.create_operation_log(
            created_by=bk_username,
            model_id=model_id,
            object_type=DataModelObjectType.MODEL.value,
            object_operation=DataModelObjectOperationType.RELEASE.value,
            object_id=model_id,
            object_name=datamodel_object.model_name,
            object_alias=datamodel_object.model_alias,
            content_before_change=latest_datamodel_content_dict,
            content_after_change=datamodel_content_dict,
        )

        # 10) 返回模型发布记录
        datamodel_release_dict = models_object_to_dict(datamodel_release_object)
        return datamodel_release_dict

    @classmethod
    def validate_model_content_in_release(cls, model_id, datamodel_content_dict, allow_no_diff=False):
        """
        模型发布时模型内容校验
        :param model_id: {int} 模型ID
        :param datamodel_content_dict: {dict} 模型当前内容
        :param allow_no_diff: {bool} 是否允许没有diff的模型再次发布
        :return: related_models_latest_version_id_dict {dict}: 关联模型最新版本信息
        """
        # 校验模型当前内容和上一个版本是否有更新
        if not allow_no_diff:
            has_diff = cls.diff_data_model(model_id)['diff']['diff_result']['has_diff']
            if not has_diff:
                raise dm_pro_errors.ModelContentHasNoChangeError()

        # 模型必须有主表，事实表模型必须有统计口径
        cls.validate_master_table_and_calatom(datamodel_content_dict)

        # 校验模型关联(按照关联维度模发布版本做校验)
        related_models_latest_version_id_dict = MasterTableManager.validate_model_relation(
            datamodel_content_dict['model_detail']['fields'], datamodel_content_dict['model_detail']['model_relation']
        )

        # 校验被引用的统计口径是否存在
        calc_atom_names_imaged = list(
            set(
                [
                    calc_atom_dict['calculation_atom_name']
                    for calc_atom_dict in datamodel_content_dict['model_detail']['calculation_atoms']
                    if calc_atom_dict['calculation_atom_type'] == CalculationAtomType.QUOTE
                ]
            )
        )
        CalculationAtomManager.validate_calc_atoms_existed(calc_atom_names_imaged)

        # 对模型当前SQL内容进行校验
        cls.validate_model_content(datamodel_content_dict)

        # 判断指标名称是否全局唯一 & 判断指标关键参数是否全局唯一
        IndicatorManager.validate_ind_existed_in_release(model_id, datamodel_content_dict['model_detail']['indicators'])

        # 对当前模型下所有指标的调度内容进行校验
        IndicatorManager.validate_ind_sche_cont_in_cur_model(model_id)
        return related_models_latest_version_id_dict

    @staticmethod
    def sync_model_con_from_stage_to_master(model_id, datamodel_content_dict, bk_username):
        """
        发布时同步草稿态模型内容到db主表
        :param model_id: {int} 模型ID
        :param datamodel_content_dict: {dict} 模型当前内容
        :param bk_username:
        :return:
        """
        # 同步草稿态主表(字段&模型关联)到db主表
        MasterTableManager.sync_mst_table_from_stage_to_master(
            model_id, datamodel_content_dict['model_detail'], bk_username
        )

        # 同步草稿态统计口径引用到db主表
        calc_atom_list = deepcopy(datamodel_content_dict['model_detail']['calculation_atoms'])
        calc_atom_image_list = []
        for calc_atom_dict in calc_atom_list:
            if calc_atom_dict['calculation_atom_type'] == CalculationAtomType.QUOTE:
                calc_atom_dict['model_id'] = model_id
                calc_atom_image_list.append(calc_atom_dict)
        CalculationAtomManager.sync_calatom_image_from_stage(model_id, calc_atom_image_list, bk_username)

        # 同步草稿态指标到db主表
        IndicatorManager.sync_inds_from_stage_to_master(
            model_id, datamodel_content_dict['model_detail']['indicators'], bk_username
        )

    @classmethod
    def diff_data_model(cls, model_id):
        """
        对比数据模型当前内容和模型上一个发布版本内容的差异
        :param model_id: {Int} 模型ID
        :return: {Dict} 模型当前内容和模型上一个发布版本内容的差异
        """
        # 1) 获取模型上一个发布版本内容和模型当前内容
        orig_datamodel_release_dict, new_datamodel_content_dict = cls.get_data_model_orig_and_cur_content(model_id)
        orig_datamodel_content_dict = orig_datamodel_release_dict.get('model_content', {})

        # 2) 获取数据模型对象列表
        orig_datamodel_object_list = get_data_model_object_content_list(orig_datamodel_content_dict)
        new_datamodel_object_list = get_data_model_object_content_list(new_datamodel_content_dict)

        # 获取模型的diff_objects和模型diff结论
        diff_objects, diff_result_dict = get_diff_objects_and_diff_result(
            orig_datamodel_object_list, new_datamodel_object_list
        )

        # 获取格式化的操作前(上一次发布) & 操作后(当前)内容
        orig_contents_dict = cls.get_format_datamodel_contents_dict(
            orig_datamodel_object_list, orig_datamodel_release_dict
        )
        new_contents_dict = {'objects': new_datamodel_object_list}

        return {
            'orig_contents': orig_contents_dict,
            'new_contents': new_contents_dict,
            'diff': {'diff_result': diff_result_dict, 'diff_objects': diff_objects},
        }

    @classmethod
    def diff_data_model_version_content(cls, model_id, orig_version_id, new_version_id):
        """
        对比数据模型不同版本内容间的差异
        :param model_id: {Int} 模型ID
        :param orig_version_id: {String} 模型ID
        :param new_version_id: {String} 模型ID
        :return: {Dict} datamodel_release_dict 模型不同版本内容间的差异
        """
        # 1) 判断模型是否存在
        get_object(DmmModelInfo, model_id=model_id)

        # 2) 获取模型不同发布版本内容
        orig_datamodel_release_dict = cls.get_model_release_con_by_ver_id(orig_version_id)
        new_datamodel_release_dict = cls.get_model_release_con_by_ver_id(new_version_id)

        orig_datamodel_content_dict = orig_datamodel_release_dict.get('model_content', {})
        new_datamodel_content_dict = new_datamodel_release_dict.get('model_content', {})

        # 3) 获取数据模型不同发布版本的对象列表
        orig_datamodel_object_list = get_data_model_object_content_list(orig_datamodel_content_dict)
        new_datamodel_object_list = get_data_model_object_content_list(new_datamodel_content_dict)

        # 4) 获取模型的diff_objects和模型diff结论
        diff_objects, diff_result_dict = get_diff_objects_and_diff_result(
            orig_datamodel_object_list, new_datamodel_object_list
        )

        # 5) 获取格式化的操作前(上一次发布) & 操作后(当前)内容
        orig_contents_dict = cls.get_format_datamodel_contents_dict(
            orig_datamodel_object_list, orig_datamodel_release_dict
        )
        new_contents_dict = cls.get_format_datamodel_contents_dict(
            new_datamodel_object_list, new_datamodel_release_dict
        )

        return {
            'orig_contents': orig_contents_dict,
            'new_contents': new_contents_dict,
            'diff': {'diff_result': diff_result_dict, 'diff_objects': diff_objects},
        }

    @staticmethod
    def get_model_release_con_by_ver_id(version_id):
        """
        根据模型版本获取模型发布内容
        :param version_id: {String} 模型版本ID
        :return: datamodel_release_dict {Dict} 模型发布内容
        """
        datamodel_release_dict = {}
        if version_id is not None:
            datamodel_release_obj = get_object(DmmModelRelease, version_id=version_id)
            datamodel_release_dict = models_object_to_dict(datamodel_release_obj)
        return datamodel_release_dict

    @staticmethod
    def get_format_datamodel_contents_dict(object_list, release_dict):
        """
        # 获取格式化的操作前(上一次发布) / 操作后(当前)内容
        :param object_list:
        :param release_dict:
        :return:
        """
        contents_dict = {'objects': object_list}
        if release_dict:
            if 'created_at' in release_dict:
                contents_dict.update({'created_at': release_dict['created_at']})
            if 'created_by' in release_dict:
                contents_dict.update(
                    {
                        'created_by': release_dict['created_by'],
                    }
                )
        return contents_dict

    @classmethod
    def get_data_model_orig_and_cur_content(cls, model_id):
        """
        获取模型上一次发布信息和当前内容
        :param model_id: {Int} 模型ID
        :return: latest_datamodel_release_dict: {Dict} 模型上一次发布信息, cur_datamodel_content_dict: {Dict} 模型当前内容
        """
        # 1) 判断模型是否存在
        datamodel_object = get_object(DmmModelInfo, model_id=model_id)

        # 2) 获取模型最新版本信息
        latest_datamodel_release_dict = {}
        if datamodel_object.latest_version_id is not None:
            datamodel_release_obj = get_object(DmmModelRelease, version_id=datamodel_object.latest_version_id)
            latest_datamodel_release_dict = models_object_to_dict(datamodel_release_obj)

        # 3) 获取模型当前内容
        cur_datamodel_content_dict = cls.get_data_model_info(
            model_id, {'with_details': ['master_table', 'calculation_atoms', 'indicators']}
        )

        return latest_datamodel_release_dict, cur_datamodel_content_dict

    @staticmethod
    def validate_master_table_and_calatom(datamodel_content_dict):
        """
        校验模型是否有主表&统计口径
        :param datamodel_content_dict:
        :return:
        """
        if not datamodel_content_dict['model_detail']['fields']:
            raise dm_pro_errors.DataModelCanNotBeReleasedError(message_kv={'error_info': _('模型未完成主表设计')})
        if (
            datamodel_content_dict['model_type'] == DataModelType.FACT_TABLE.value
            and not datamodel_content_dict['model_detail']['calculation_atoms']
        ):
            raise dm_pro_errors.DataModelCanNotBeReleasedError(message_kv={'error_info': _('事实表模型未完成统计口径设计')})

    @classmethod
    def get_data_model_release_list(cls, model_id):
        """
        模型发布列表
        :param model_id:
        :return:
        """
        release_queryset = (
            DmmModelRelease.objects.filter(model_id=model_id)
            .order_by('-created_at')
            .values('created_by', 'created_at', 'version_log', 'version_id')
        )

        return release_queryset

    @classmethod
    def get_data_model_overview_info(cls, model_id, latest_version=False):
        """
        获取模型预览信息
        :param model_id: {Int}: 模型ID
        :param latest_version: {bool} 返回模型草稿态/最新发布版本信息，默认返回模型草稿态信息
        :return: {Dict}: {'nodes':[], 'lines':[]}
        """
        return DataModelOverviewManager.get_data_model_overview_info(model_id, latest_version=latest_version)

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def confirm_data_model_overview(cls, model_id, bk_username):
        """
        数据模型确认预览
        :param model_id: {Int} 模型ID
        :param bk_username: {String} 用户名
        :return: {Int} 模型构建&发布完成步骤
        """
        return DataModelOverviewManager.confirm_data_model_overview(model_id, bk_username)

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def confirm_indicators(cls, model_id, bk_username):
        """
        确认指标
        :param model_id: {Int} 模型ID
        :param bk_username: {String} 用户名
        :return: step_id: {Int} 模型构建&完成步骤
        """
        # 更新模型step_id状态
        step_id, _ = update_data_model_state(model_id, cls.confirm_indicators.__name__, bk_username)
        return step_id

    @staticmethod
    def is_datamodels_top(bk_username):
        """
        判断模型是否置顶
        :param bk_username: {String} 用户名
        :return: top_datamodel_dict: {Dict} 模型是否置顶
        """
        top_datamodel_queryset = DmmModelTop.objects.filter(created_by=bk_username)
        top_datamodel_dict = {}
        for top_datamodel in top_datamodel_queryset:
            top_datamodel_dict[top_datamodel.model_id] = True
        return top_datamodel_dict

    @classmethod
    def get_dim_model_list_can_be_related(cls, model_id, related_model_id=None, published=True):
        """
        草稿态获取可以关联的维度模型列表
        :param model_id: {int} 模型ID
        :param related_model_id: {int} 关联模型ID,用于前端打开已经关联的维度表数据模型时回填,默认为None
        :param published: {bool} 是否只返回已经发布的维度模型，默认只返回已经发布的维度模型
        :return: dim_model_queryset: {queryset} 可以关联的模型queryset
        """
        relation_queryset = DmmModelRelationStage.objects.filter(model_id=model_id)

        # exclude已经关联的维度模型
        exclude_model_ids = [
            relation_obj.related_model_id
            for relation_obj in relation_queryset
            if relation_obj.related_model_id != related_model_id
        ]
        exclude_model_ids.append(model_id)

        # 过滤出已经发布的维度模型
        condition_params = {}
        if published:
            condition_params['publish_status__in'] = [
                DataModelPublishStatus.PUBLISHED.value,
                DataModelPublishStatus.REDEVELOPING.value,
            ]
        dim_model_queryset = (
            DmmModelInfo.objects.filter(
                model_type=DataModelType.DIMENSION_TABLE.value, step_id__gte=DataModelStep.MASTER_TABLE_DONE.value
            )
            .filter(**condition_params)
            .exclude(model_id__in=exclude_model_ids)
            .values('model_id', 'model_name', 'model_alias', 'project_id')
        )

        # 判断维度模型除主键和时间字段外是否还有其他维度字段，若无则在关联维度模型时禁选
        dim_model_ids = []
        dmm_model_list = []
        for dim_model_dict in dim_model_queryset:
            dim_model_ids.append(dim_model_dict['model_id'])
            dmm_model_list.append(dim_model_dict)
        # 获取除了主键和时间字段还有其他维度字段的维度模型model_ids
        dim_model_ids_has_extended_fields = [
            dim_model_count_dict['model_id']
            for dim_model_count_dict in DmmModelField.objects.filter(model_id__in=dim_model_ids)
            .exclude(is_primary_key=True)
            .exclude(field_name=TimeField.TIME_FIELD_NAME)
            .exclude(field_type=TimeField.TIME_FIELD_TYPE)
            .values('model_id')
            .annotate(count=Count('model_id'))
        ]
        for dmm_model_dict in dmm_model_list:
            dmm_model_dict['has_extended_fields'] = False
            if dmm_model_dict['model_id'] in dim_model_ids_has_extended_fields:
                dmm_model_dict['has_extended_fields'] = True
        return dmm_model_list

    @classmethod
    def get_data_model_list(cls, params):
        """
        get数据模型列表
        :param params: {Dict} 数据模型列表过滤参数
        :return: datamodel_list: {List} 数据模型列表
        """
        # 1) 按照params过滤模型列表
        bk_username = params.pop('bk_username')
        keyword = params.pop('keyword') if 'keyword' in params else None
        # 按照project_id & model_type & model_id & model_type过滤
        datamodel_queryset = DmmModelInfo.objects.filter(**params).order_by('-updated_at')

        if keyword:
            # 按照模型名称/模型别名/模型描述过滤
            datamodel_filterd_keyword_queryset = datamodel_queryset.filter(
                Q(model_name__icontains=keyword) | Q(model_alias__icontains=keyword) | Q(description__icontains=keyword)
            )

            # 按照标签名称/标签别名过滤(查标签名称/标签别名对应的实体)
            filtered_tag_model_ids = get_datamodels_by_tag(keyword)
            datamodel_filterd_tag_queryset = datamodel_queryset.filter(model_id__in=filtered_tag_model_ids)

            # 按照模型名称/模型别名/模型描述过滤 和 按照标签名称/标签别名过滤 求并集
            datamodel_queryset = datamodel_filterd_keyword_queryset.union(datamodel_filterd_tag_queryset)

        # 2) 返回模型列表参数
        model_ids = [datamodel_object.model_id for datamodel_object in datamodel_queryset]
        # 批量获取模型对应的tag,包含tag_alias
        tag_target_dict = get_datamodel_tags(model_ids, with_tag_alias=True)
        # 批量判断模型用户是否置顶模型
        top_datamodel_dict = cls.is_datamodels_top(bk_username)

        # 3) 获取模型属性 & 判断模型是否可以被删除
        # 获取被关联的模型列表 & 被其他模型引用的模型列表 & 模型应用数量applied_count，用于判断模型是否可以被删除
        related_model_ids, quoted_model_ids, applied_count_dict = cls.get_model_used_info()

        datamodel_list = []
        for datamodel_object in datamodel_queryset:
            model_id = datamodel_object.model_id
            # 模型应用数量applied_count, 待模型应用启动加逻辑
            datamodel_dict = models_object_to_dict(
                datamodel_object,
                tags=tag_target_dict.get(model_id, []),
                sticky_on_top=top_datamodel_dict.get(model_id, False),
                applied_count=applied_count_dict.get(model_id, 0),
            )
            datamodel_list.append(datamodel_dict)

        # 模型能否被删除 & 相关属性
        cls.set_datamodel_deletable(datamodel_list, related_model_ids, quoted_model_ids, applied_count_dict)

        return datamodel_list

    @classmethod
    def get_model_used_info(cls):
        """
        获取被关联的模型列表 & 被其他模型引用的模型列表 & 模型应用数量
        :return:
        """
        # 被关联的模型列表
        model_relation_queryset = DmmModelRelation.objects.all()
        related_model_ids = [model_relation_obj.related_model_id for model_relation_obj in model_relation_queryset]

        # 被其他模型引用的模型列表
        calc_atom_image_queryset = DmmModelCalculationAtomImage.objects.all()
        calc_atom_names = [
            calc_atom_image_obj.calculation_atom_name for calc_atom_image_obj in calc_atom_image_queryset
        ]
        calc_atom_queryset = DmmModelCalculationAtom.objects.filter(calculation_atom_name__in=calc_atom_names)
        quoted_model_ids = [calc_atom_obj.model_id for calc_atom_obj in calc_atom_queryset]

        # 模型应用数量字典
        applied_count_dict = cls.get_applied_count_dict()
        return related_model_ids, quoted_model_ids, applied_count_dict

    @staticmethod
    def get_applied_count_dict():
        """
        返回模型应用数量
        :return: {Dict} 模型应用数量
        """
        applied_count_queryset = (
            DmmModelInstance.objects.all().values('model_id').annotate(applied_count=Count('model_id'))
        )
        applied_count_dict = {
            applied_dict['model_id']: applied_dict['applied_count'] for applied_dict in applied_count_queryset
        }
        return applied_count_dict

    @staticmethod
    def set_datamodel_deletable(datamodel_list, related_model_ids, quoted_model_ids, applied_count_dict):
        """
        模型能否被删除 & 相关参数(模型是否被其他模型关联 & 模型统计口径是否被其他模型引用 & 模型统计口径是否被实例化)
        :param datamodel_list: {List} 模型详情列表
        :param related_model_ids: {List} 被关联的模型列表
        :param quoted_model_ids: {List} 被其他模型引用的模型列表
        :param applied_count_dict: {dict} 模型应用数据字典
        :return:
        """
        for datamodel_dict in datamodel_list:
            datamodel_dict.update(
                {
                    'is_related': True if datamodel_dict['model_id'] in related_model_ids else False,
                    'is_quoted': True if datamodel_dict['model_id'] in quoted_model_ids else False,
                    # 模型统计口径是否被实例化，后续补充是否被实例化的逻辑
                    'is_instantiated': True if datamodel_dict['model_id'] in applied_count_dict else False,
                }
            )
            datamodel_dict['deletable'] = (
                False
                if (datamodel_dict['is_related'] or datamodel_dict['is_quoted'] or datamodel_dict['is_instantiated'])
                else True
            )

    @staticmethod
    def is_datamodel_top_object_existed(model_id, bk_username):
        """
        置顶模型时, 判断用户是否已经置顶模型, 已经置顶返回True
        :param model_id: {Int} 模型ID
        :param bk_username: {String} 用户名
        :return:
        """
        if DmmModelTop.objects.filter(model_id=model_id, created_by=bk_username).count() > 0:
            logger.info('data model:{} is already sticky on top for user:{} '.format(model_id, bk_username))
            return True
        return False

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def top_data_model(cls, model_id, bk_username):
        """
        模型置顶
        :param model_id: {Int} 模型ID
        :param bk_username: {String} 用户名
        :return: Boolean,是否成功
        """
        # 1) 判断模型是否存在
        get_object(DmmModelInfo, model_id=model_id)

        # 2）判断用户是否已经置顶模型,已经置顶就直接返回
        is_datamodel_top = cls.is_datamodel_top_object_existed(model_id, bk_username)
        # 已经置顶了模型就不处理直接返回
        if is_datamodel_top:
            return True

        # 3）置顶
        # 用户置顶的数量最大为MAX_TOP_NUM,如果超过最大置顶数,就把该用户最早置顶的结果删除
        top_datamodel_queryset = DmmModelTop.objects.filter(created_by=bk_username).order_by('created_at')
        if top_datamodel_queryset.count() >= MAX_TOP_NUM:
            for i in range(top_datamodel_queryset.count() - MAX_TOP_NUM + 1):
                top_datamodel_queryset[i].delete()
        top_datamodel_object = DmmModelTop(
            model_id=model_id,
            created_by=bk_username,
            updated_by=bk_username,
        )
        top_datamodel_object.save()
        return True

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def cancel_top_data_model(cls, model_id, bk_username):
        """
        模型取消置顶
        :param model_id: {Int} 模型ID
        :return: {Boolean} 是否成功
        """
        # 1) 判断模型是否存在
        get_object(DmmModelInfo, model_id=model_id)

        # 2）判断模型是否置顶,已经置顶则取消
        DmmModelTop.objects.filter(model_id=model_id, created_by=bk_username).delete()
        return True

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def delete_data_model(cls, model_id, bk_username):
        """
        软删除数据模型,将model_name加delete-4位随机序列-前缀,删除模型内容
        :param model_id: {Int} 模型ID
        :param bk_username: {String} 用户名
        :return:
        """
        # 1) 判断model_id对应的数据模型是否存在
        datamodel_object = get_object(DmmModelInfo, use_origin_objects=True, model_id=model_id)
        if datamodel_object.active_status == DataModelActiveStatus.DISABLED.value:
            raise dm_pro_errors.DataModelAlreadyDELETEDError

        # 2) 判断模型是否可以删除
        cls.can_data_model_be_deleted(model_id, datamodel_object)

        # 3) 获取数据模型删除前模型详情
        orig_datamodel_dict = cls.get_data_model_info(
            model_id, {'with_details': ['master_table', 'calculation_atoms', 'indicators']}
        )

        # 4) 删除数据模型上的标签
        delete_model_tag(model_id, bk_username)

        # 5) 看模型是否置顶，若有模型置顶则取消置顶
        DmmModelTop.objects.filter(model_id=model_id).delete()

        # 6) 删除主表、统计口径、指标信息
        # 删除模型下的所有指标
        IndicatorManager.delete_all_indicators_in_model(model_id)
        # 删除统计口径
        CalculationAtomManager.delete_all_calc_atoms_in_model(model_id)
        # 删除主表和关联关系
        MasterTableManager.delete_master_table(model_id)

        # 7) 软删除数据模型, 将model_name加delete-4位随机序列-前缀, 防止重新创建同名模型的时候不符合model_name的唯一值约束
        datamodel_object.active_status = DataModelActiveStatus.DISABLED.value
        datamodel_object.model_name = 'delete-{}-{}'.format(
            get_random_string(DELETED_MODEL_NAME_RANDOM_LEN), datamodel_object.model_name
        )
        datamodel_object.updated_by = bk_username
        datamodel_object.save()

        # 8) 返回软删除后的模型信息
        datamodel_dict = models_object_to_dict(datamodel_object, tags=[])

        # 9) 增加操作记录
        OperationLogManager.create_operation_log(
            created_by=bk_username,
            model_id=datamodel_dict['model_id'],
            object_type=DataModelObjectType.MODEL.value,
            object_operation=DataModelObjectOperationType.DELETE.value,
            object_id=datamodel_dict['model_id'],
            object_name=datamodel_dict['model_name'],
            object_alias=datamodel_dict['model_alias'],
            content_before_change=orig_datamodel_dict,
            content_after_change=datamodel_dict,
        )
        return datamodel_dict

    @classmethod
    def can_data_model_be_deleted(cls, model_id, datamodel_object):
        """
        获取模型是否可以删除
        :param model_id: {Int} 模型ID
        :param datamodel_object: {Object} DmmModelInfo对应的object
        :return:
        """
        # 获取被关联的模型列表 & 被其他模型引用的模型列表，用于判断模型是否可以被删除
        related_model_ids, quoted_model_ids, applied_count_dict = cls.get_model_used_info()
        datamodel_dict = models_object_to_dict(datamodel_object)
        # 模型能否被删除 & 相关属性
        cls.set_datamodel_deletable([datamodel_dict], related_model_ids, quoted_model_ids, applied_count_dict)
        if not datamodel_dict['deletable']:
            error_info = cls.get_data_model_deleted_error_info(datamodel_dict)
            raise dm_pro_errors.DataModelCanNotBeDeletedError(
                message_kv={'model_id': model_id, 'error_info': error_info}
            )

    @staticmethod
    def get_data_model_deleted_error_info(datamodel_dict):
        """
        获取模型被删除的错误信息
        :param datamodel_dict: {Dict} 模型
        :return:
        """
        error_info_list = []
        for key, value in list(DataModelUsedInfoDict.items()):
            if datamodel_dict[key]:
                error_info_list.append(str(value))
        error_info = '&'.join(error_info_list)
        error_info = _('模型被{}'.format(error_info))
        return error_info


class DataModelOverviewManager(object):
    @classmethod
    def get_data_model_overview_info(cls, model_id, latest_version=False):
        """
        获取模型预览信息
        :param model_id: {int}: 模型ID
        :param latest_version: {bool} 返回模型草稿态/最新发布版本信息，latest_version=False默认返回模型草稿态信息
        :return: {dict}: {'nodes':[], 'lines':[]}
        """
        # 1) 模型预览节点信息
        nodes, fact_model_dict, calculation_atom_list, indicator_list, node_id_dict = (
            cls.get_latest_ver_nodes_and_cont_in_ov(model_id)
            if latest_version
            else cls.get_nodes_and_related_content_in_ov(model_id)
        )

        # 2) 模型预览边信息
        lines = cls.get_lines_in_overview(
            model_id, fact_model_dict, calculation_atom_list, indicator_list, node_id_dict
        )
        return {'nodes': nodes, 'lines': lines}

    @classmethod
    def get_latest_ver_nodes_and_cont_in_ov(cls, model_id):
        """
        获取模型预览中的节点列表和相关内容(最新发布版本)
        :param model_id: {int} 模型ID
        :return: nodes:{list}节点列表, fact_model_dict:{dict}事实表信息, calculation_atom_list:{list}统计口径列表,
                        indicator_list:{list}指标列表, node_id_dict:{dict}节点ID字典
        """
        # 1）获取模型最新发布预览展示的信息
        data_model_dict = DataModelManager.get_model_latest_ver_content_dict(model_id)
        if not data_model_dict or 'model_detail' not in data_model_dict:
            raise dm_pro_errors.DataModelHasNotBeenReleasedError()
        # a) 获取主表信息
        data_model_dict['fields'] = data_model_dict['model_detail']['fields']
        field_alias_dict = {
            field_dict['field_name']: field_dict['field_alias'] for field_dict in data_model_dict['fields']
        }
        # b) 获取维度表信息
        related_model_version_id_dict = {}
        related_model_ids_without_latest_version = []

        for relation_dict in data_model_dict['model_detail']['model_relation']:
            # 获取发布过的被关联的维度表version_id
            if relation_dict.get('related_model_version_id', None) is not None:
                related_model_version_id_dict[relation_dict['related_model_id']] = relation_dict[
                    'related_model_version_id'
                ]
            # 获取未发布过的被关联的维度表model_id
            else:
                related_model_ids_without_latest_version.append(relation_dict['related_model_id'])
        # 发布过的被关联模型
        related_model_list = [
            DataModelManager.get_model_latest_ver_content_dict(related_model_id, related_model_version_id)
            for related_model_id, related_model_version_id in list(related_model_version_id_dict.items())
        ]
        for related_model_dict in related_model_list:
            related_model_dict['fields'] = related_model_dict['model_detail']['fields']
        # 未发布过的被关联模型
        related_model_list += [
            cls.add_model_info_and_fields(related_model_id)
            for related_model_id in related_model_ids_without_latest_version
        ]
        # c) 获取统计口径信息
        calculation_atom_list = data_model_dict['model_detail']['calculation_atoms']
        calc_atom_alias_dict = {}
        # 补充统计口径字段信息 & 统计口径中文名称字典
        for calc_atom_dict in calculation_atom_list:
            # 用于指标返回统计口径中文名
            calc_atom_alias_dict[calc_atom_dict['calculation_atom_name']] = calc_atom_dict['calculation_atom_alias']
            calc_atom_dict['fields'] = [{'field_name': strip_comment(calc_atom_dict['calculation_formula'])}]
        # d) 获取指标信息
        indicator_list = data_model_dict['model_detail']['indicators']
        # 指标字段信息
        for indicator_dict in indicator_list:
            IndicatorManager.set_indicator_fields(indicator_dict, calc_atom_alias_dict, field_alias_dict)

        # 2）获取模型预览的所有节点
        nodes = cls.get_nodes_to_overview(data_model_dict, related_model_list, calculation_atom_list, indicator_list)

        # 3）获取主表/关联表节点id字典，用于连线
        node_id_dict = cls.get_mst_and_rel_models_node_id_dict(nodes)
        return nodes, data_model_dict, calculation_atom_list, indicator_list, node_id_dict

    @staticmethod
    def get_mst_and_rel_models_node_id_dict(nodes):
        """
        获取主表/关联表节点id信息，用于连线
        :param nodes: {list}，预览中的节点列表
        :return:
        """
        node_id_dict = {}
        for node_dict in nodes:
            if node_dict['node_type'] in DataModelType.get_enum_value_list():
                node_id_dict[node_dict['model_id']] = node_dict['node_id']
        return node_id_dict

    @classmethod
    def get_nodes_and_related_content_in_ov(cls, model_id):
        """
        获取模型预览中的节点列表及相关内容(草稿态)
        :param model_id: {int} 模型ID
        :return: nodes:{list}节点列表, fact_model_dict:{dict}事实表信息, calculation_atom_list:{list}统计口径列表,
        indicator_list:{list}指标列表, node_id_dict:{dict}节点ID字典
        """
        # 1）获取模型预览展示的信息
        # a) 获取主表信息
        master_model_dict = cls.add_model_info_and_fields(model_id)
        # b) 获取关联表信息
        related_model_list = []
        for relation_dict in master_model_dict['model_detail']['model_relation']:
            try:
                get_object(DmmModelInfo, model_id=relation_dict['related_model_id'])
            except dm_pro_errors.DataModelNotExistError as e:
                logger.error('related_dimension_model model_id:{}'.format(relation_dict['related_model_id']))
                raise e.__class__(message=_('关联模型model_id:{}不存在'.format(relation_dict['related_model_id'])))
            related_model_list.append(cls.add_model_info_and_fields(relation_dict['related_model_id']))

        # c) 获取统计口径信息
        calculation_atom_list, step_id, publish_status = CalculationAtomManager.get_calculation_atom_list(
            {'model_id': model_id, 'with_details': ['fields']}
        )
        # d) 获取指标信息
        indicator_list, step_id, publish_status = IndicatorManager.get_indicator_list(
            {'model_id': model_id, 'with_details': ['fields']}
        )

        # 2）获取模型预览的所有节点
        nodes = cls.get_nodes_to_overview(master_model_dict, related_model_list, calculation_atom_list, indicator_list)

        # 3）主表/关联表节点id信息，用于连线
        node_id_dict = cls.get_mst_and_rel_models_node_id_dict(nodes)
        return nodes, master_model_dict, calculation_atom_list, indicator_list, node_id_dict

    @classmethod
    def get_nodes_to_overview(cls, master_model_dict, related_model_list, calculation_atom_list, indicator_list):
        """
        获取模型预览的所有节点
        :param master_model_dict: {dict} 事实模型信息
        :param related_model_list: {list} 被关联维度模型
        :param calculation_atom_list: {list} 统计口径列表
        :param indicator_list: {list} 指标列表
        :return: nodes: {list},节点列表
        """
        nodes = []
        # 添加模型主表节点 & 添加模型关联表节点 & 统计口径节点 & 指标节点
        for node_list, node_type, node_name in [
            ([master_model_dict], DataModelType.FACT_TABLE.value, 'model_name'),
            (related_model_list, DataModelType.DIMENSION_TABLE.value, 'model_name'),
            (calculation_atom_list, DataModelObjectType.CALCULATION_ATOM.value, 'calculation_atom_name'),
            (indicator_list, DataModelObjectType.INDICATOR.value, 'indicator_name'),
        ]:
            for node_dict in node_list:
                cls.add_node_to_overview(node_dict, nodes, node_type, node_dict[node_name])
        return nodes

    @classmethod
    def add_model_info_and_fields(cls, model_id):
        """
        获取模型主表/关联维度表基本信息&字段列表，用于模型预览结果展示
        :param model_id: {int} 模型ID
        :return: data_model_dict:{dict} 主表/关联维度表基本信息&字段列表
        """
        # 1）模型详情
        data_model_dict = DataModelManager.get_data_model_info(model_id, {'with_details': ['master_table']})
        # 2）模型字段列表
        data_model_dict['fields'] = data_model_dict['model_detail']['fields']

        return data_model_dict

    @staticmethod
    def get_lines_in_overview(model_id, fact_model_dict, calculation_atom_list, indicator_list, node_id_dict):
        """
        获取模型预览中的连线列表
        :param model_id: {Int} 模型ID
        :param fact_model_dict: {Dict} 事实表模型字典
        :param calculation_atom_list: {List} 统计口径列表
        :param indicator_list: {List} 指标列表
        :param node_id_dict: {Dict} {模型ID:节点ID}
        :return: lines {List} 连线列表
        """
        lines = []
        for relation_dict in fact_model_dict['model_detail']['model_relation']:
            lines.append(
                {
                    'from': node_id_dict[relation_dict['related_model_id']],
                    'to': node_id_dict[relation_dict['model_id']],
                    'from_field_name': relation_dict['related_field_name'],
                    'to_field_name': relation_dict['field_name'],
                }
            )
        for calculation_atom_dict in calculation_atom_list:
            lines.append(
                {
                    'from': node_id_dict[int(model_id)],
                    'to': '{}-{}'.format(
                        DataModelObjectType.CALCULATION_ATOM.value, calculation_atom_dict['calculation_atom_name']
                    ),
                }
            )
        for indicator_dict in indicator_list:
            if indicator_dict['parent_indicator_name'] is None:
                lines.append(
                    {
                        'from': '{}-{}'.format(
                            DataModelObjectType.CALCULATION_ATOM.value, indicator_dict['calculation_atom_name']
                        ),
                        'to': '{}-{}'.format(DataModelObjectType.INDICATOR.value, indicator_dict['indicator_name']),
                    }
                )
            # 衍生指标
            else:
                lines.append(
                    {
                        'from': '{}-{}'.format(
                            DataModelObjectType.INDICATOR.value, indicator_dict['parent_indicator_name']
                        ),
                        'to': '{}-{}'.format(DataModelObjectType.INDICATOR.value, indicator_dict['indicator_name']),
                    }
                )
        return lines

    @staticmethod
    def set_node_id_and_type_in_overview(node_dict, node_type, node_name):
        """
        节点详情中添加node_type和node_id属性
        :param node_dict: {Dict} 节点详情
        :param node_type: {String} 节点类型
        :param node_name: {String} 节点ID
        :return:
        """
        node_dict['node_type'] = node_type
        node_dict['node_id'] = '{}-{}'.format(node_type, node_name)

    @classmethod
    def add_node_to_overview(cls, node_dict, nodes, node_type, node_name):
        """
        节点信息中添加node_type和node_id属性，并在数据预览中增加节点信息
        :param node_dict: {Dict} 节点详情
        :param nodes: {List} 节点列表
        :param node_type: {String} 节点类型
        :param node_name: {String} 节点ID
        :return:
        """
        cls.set_node_id_and_type_in_overview(node_dict, node_type, node_name)
        nodes.append(node_dict)

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def confirm_data_model_overview(cls, model_id, bk_username):
        """
        数据模型确认预览接口
        :param model_id:
        :param bk_username:
        :return:
        """
        # 更新模型step_id状态
        step_id, _ = update_data_model_state(model_id, cls.confirm_data_model_overview.__name__, bk_username)
        return step_id
