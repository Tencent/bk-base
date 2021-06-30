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
from django.utils.translation import ugettext as _

from common.transaction import auto_meta_sync
from common.log import logger

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.datamodel import (
    DmmModelInfo,
    DmmModelField,
    DmmModelRelation,
    DmmModelFieldStage,
    DmmModelRelationStage,
)
from datamanage.pro.datamodel.models.indicator import DmmModelIndicatorStage

from datamanage.pro.datamodel.utils import list_diff
from datamanage.pro.datamodel.handlers.verifier import SQLVerifier
from datamanage.pro.datamodel.handlers.constraint import validate_field_constraint_content
from datamanage.pro.datamodel.models.model_dict import (
    FieldCategory,
    DataModelType,
    TimeField,
    DataModelObjectType,
    DataModelObjectOperationType,
    DataModelActiveStatus,
    DATA_MODEL_UNIQUE_KEY_MAPPINGS,
)
from datamanage.pro.datamodel.dmm.object import (
    get_object,
    get_added_updated_deleted_objects,
    get_unique_key,
    update_objects,
    models_object_to_dict,
)
from datamanage.pro.datamodel.dmm.state_machine import update_data_model_state
from datamanage.pro.datamodel.dmm.calculation_atom_manager import CalculationAtomManager
from datamanage.pro.datamodel.dmm.operation_log_manager import OperationLogManager

DELETED_MODEL_NAME_PREFIX_LEN = 12


class MasterTableManager(object):
    @staticmethod
    def validate_measure_and_primary_key(fields, model_type):
        """
        校验主键和度量是否符合主表要求
        :param fields: {List} 字段列表
        :param model_type: {String} 字段类型
        :return: has_measure: {Boolean} 是否有度量, has_primary_key:{Boolean} 是否有主键
        """
        has_measure = False
        has_primary_key = False
        for field_dict in fields:
            if field_dict['field_category'] == FieldCategory.MEASURE.value:
                has_measure = True
            elif field_dict['is_primary_key']:
                has_primary_key = True
            if has_measure and has_primary_key:
                break
        # 维度模型必须有主键，不能有度量
        if model_type == DataModelType.DIMENSION_TABLE.value:
            if not has_primary_key:
                raise dm_pro_errors.DimensionTableNotHaveMetricError
            if has_measure:
                raise dm_pro_errors.DimensionTableHaveMeasureError
        return has_measure, has_primary_key

    @classmethod
    def validate_field_clean_content(cls, table_name, fields):
        """
        校验字段加工逻辑
        :param table_name: {String} 主表名称
        :param fields: {List} 主表字段列表
        :return:
        """
        schema_list = []
        sql_dict = {}
        # timestamp是保留关键字,用于SQLVerifier报错
        for field_dict in fields:
            if (
                field_dict['field_name'] != TimeField.TIME_FIELD_NAME
                and field_dict['field_type'] != TimeField.TIME_FIELD_TYPE
            ):
                schema_list.append({'field_type': field_dict['field_type'], 'field_name': field_dict['field_name']})
                sql_dict[field_dict['field_name']] = (
                    field_dict['field_clean_content']['clean_content']
                    if field_dict['field_clean_content']
                    else field_dict['field_name']
                )

        schema_dict = {table_name: schema_list}
        clean_content_dict = {"scopes": schema_dict, "sql_parts": {"fields": sql_dict, "table_name": table_name}}
        # sql片段校验
        verifier = SQLVerifier(**clean_content_dict)
        matched, match_info = verifier.check_res_schema(schema_dict, 'flink_sql', agg=False, extract_source_column=True)
        if not matched:
            raise dm_pro_errors.SqlValidateError(
                message_kv={'error_info': '{} [ERROR_SQL: {}]'.format(match_info, verifier.show_sql)}
            )
        return match_info['source_columns']

    @staticmethod
    def validate_field_in_master_table(fields, model_relation):
        """
        判断模型关联中的主表关联字段是否在主表中
        :param fields: {List} 主表字段
        :param model_relation: {List} 模型关联
        :return:
        """
        field_name_list = [field_dict['field_name'] for field_dict in fields]
        related_field_name_list = [relation_dict['field_name'] for relation_dict in model_relation]
        # 判断关联字段是否是主表字段的子集
        if not set(related_field_name_list).issubset(field_name_list):
            raise dm_pro_errors.FieldNotInMasterTableError(message_kv={'field_name': related_field_name_list})

    @classmethod
    def validate_related_model_id_and_field(cls, related_model_list, **kwargs):
        """
        判断related_model_id/source_model_id对应的模型是否存在, related_field_name/source_field_name是否在model_id对应的主表中
        :param related_model_list: {List} 关联模型id和对应关联字段名称列表 [{'model_id': 1, 'field_name':'price'}]
        :param kwargs: {Dict} 额外过滤条件 例如,被关联维度表的关联字段类型只能是主键 {'is_primary_key':True}
        :return:
        """
        related_model_ids = []
        related_field_names = []
        model_id_field_name_list = []
        for each_related_model_dict in related_model_list:
            related_model_ids.append(each_related_model_dict['model_id'])
            related_field_names.append(each_related_model_dict['field_name'])
            model_id_field_name_list.append(
                (each_related_model_dict['model_id'], each_related_model_dict['field_name'])
            )

        # related_model_ids中存在的model_ids
        orig_model_ids = []
        model_version_id_dict = {}
        for dmm_model in DmmModelInfo.objects.filter(
            model_id__in=related_model_ids, model_type=DataModelType.DIMENSION_TABLE.value
        ):
            orig_model_ids.append(dmm_model.model_id)
            model_version_id_dict[dmm_model.model_id] = dmm_model.latest_version_id

        # 判断模型是否存在
        add_model_ids, delete_model_ids, update_model_ids = list_diff(orig_model_ids, related_model_ids)
        if add_model_ids:
            raise dm_pro_errors.DataModelNotExistError(message=_('关联模型不存在'))

        orig_fields = [
            (field_obj.model_id, field_obj.field_name)
            for field_obj in DmmModelField.objects.filter(model_id__in=related_model_ids).filter(**kwargs)
        ]
        # 判断字段是否存在
        add_fields, delete_fields, update_fields = list_diff(orig_fields, model_id_field_name_list)
        if add_fields:
            raise dm_pro_errors.FieldNotExistError(message=_('关联、扩展字段不存在/关联、扩展字段类型校验不通过异常'))
        return model_version_id_dict

    @classmethod
    def validate_master_table(cls, table_name, model_type, fields, model_relation):
        """
        主表校验
        :param table_name: {String} 主表名称
        :param model_type: {String} 主表类型
        :param fields: {List} 字段列表
        :param model_relation: {List} 模型关联
        :return: origin_fields_dict: {Dict} 主表字段加工SQL来源字段
        """
        # 1) 主表校验
        # a) 校验主键和度量是否符合要求: 1.维度表必须有主键 2.维度表不能有度量
        cls.validate_measure_and_primary_key(fields, model_type)
        # b) 校验字段约束
        validate_field_constraint_content(fields)
        # c) 校验字段加工方式SQL
        origin_fields_dict = cls.validate_field_clean_content(table_name, fields)

        # 2) 关联字段校验 & 模型关联校验
        # a) 判断模型关联中的主表关联字段是否在主表中
        cls.validate_field_in_master_table(fields, model_relation)
        # b) 校验模型关联(按照关联维度模发布版本做校验)
        cls.validate_model_relation(fields, model_relation)

        return origin_fields_dict

    @classmethod
    def validate_model_relation(cls, fields, model_relation):
        """
        校验模型关联(按照关联维度模型的最新发布版本做校验)
        :param fields: {list} 字段列表
        :param model_relation: {list} 模型关联列表
        :return:related_model_version_id_dict {dict}: 被关联维度模型的版本id信息
        """
        # 1) 校验source_model_id对应的维度表模型存在, 判断source_field_name是否在source_model_id对应的主表中
        source_model_list = [
            {'model_id': field_dict['source_model_id'], 'field_name': field_dict['source_field_name']}
            for field_dict in fields
            if field_dict['source_model_id'] and field_dict['source_field_name']
        ]
        cls.validate_related_model_id_and_field(source_model_list, field_category='dimension')

        # 2) 判断related_model_id对应的维度表模型存在, 判断related_field_name是否在related_model_id对应的主表中
        related_model_list = [
            {'model_id': relation_dict['related_model_id'], 'field_name': relation_dict['related_field_name']}
            for relation_dict in model_relation
        ]
        related_model_version_id_dict = cls.validate_related_model_id_and_field(related_model_list, is_primary_key=True)
        return related_model_version_id_dict

    @classmethod
    def validate_fields_editable_deletable(cls, model_id, deleted_objects, updated_objects, origin_fields_dict=None):
        """
        校验字段是否可以删除&修改
        :param model_id: 模型ID
        :type model_id: int
        :param deleted_objects: 删除的models object列表
        :type deleted_objects: [object]
        :param updated_objects: 修改数据类型的models object列表
        :type updated_objects: [object]
        :param origin_fields_dict: 主表字段对应的origin_fields mappings
        :return:
        """
        fields = cls.get_master_table_info(
            model_id,
            with_time_field=True,
            with_details=['deletable', 'editable'],
            origin_fields_dict=origin_fields_dict,
        )['fields']
        field_names_can_be_deleted = []
        field_names_field_type_can_be_edited = []
        for field_dict in fields:
            if field_dict['deletable']:
                field_names_can_be_deleted.append(field_dict['field_name'])
            if field_dict['editable_field_type']:
                field_names_field_type_can_be_edited.append(field_dict['field_name'])
        for delete_obj in deleted_objects:
            if delete_obj.field_name not in field_names_can_be_deleted:
                raise dm_pro_errors.FieldCanNotBeDeletedError(message_kv={'field_name': delete_obj.field_name})
        for update_obj in updated_objects:
            # 是否可以改变数据类型
            if update_obj.field_name not in field_names_field_type_can_be_edited:
                raise dm_pro_errors.FieldCanNotBeEditedError(message_kv={'field_name': update_obj.field_name})

    @classmethod
    def update_master_table_object(
        cls, model_id, models_cls, params_list, bk_username, attr_names, origin_fields_dict=None, **specific_attrs
    ):
        """
        主表字段 & 模型关联关系 修改、新增、删除
        :param model_id: {Int} 模型ID
        :param models_cls: {Class} models对应的class,例如DmmModelField
        :param params_list: {List} 参数列表
        :param bk_username: {String} 用户名
        :param attr_names: {List} 用于唯一标示object的属性名列表 ['field_name']
        :param origin_fields_dict: 主表字段对应的origin_fields mappings
        :param specific_attrs: {Dict} 参数列表对应数据库没有的字段字典，例如 {'model_id':1}
        :return:
        """
        # 1）获取待新增/删除/变更的object_list
        added_objects, deleted_objects, updated_objects, orig_object_dict = get_added_updated_deleted_objects(
            model_id, models_cls, params_list, attr_names, **specific_attrs
        )

        # 2）校验编辑/删除的字段是否可以编辑/删除
        if models_cls == DmmModelFieldStage:
            # 编辑的字段中修改数据类型的字段列表
            update_field_type_objects = [
                update_obj
                for update_obj in updated_objects
                if update_obj.field_type != orig_object_dict[get_unique_key(update_obj, attr_names)].field_type
            ]
            # 校验待删除字段是否可以删除&待修改字段是否可以修改
            cls.validate_fields_editable_deletable(
                model_id, deleted_objects, update_field_type_objects, origin_fields_dict
            )

        # 3) 对待新增/删除/变更的字段/模型关联分别进行新增、删除、修改
        has_update = update_objects(
            orig_object_dict, added_objects, updated_objects, deleted_objects, attr_names, bk_username, **specific_attrs
        )
        return has_update

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def update_master_table(cls, model_id, params, bk_username):
        """
        草稿态创建/修改主表
        :param params: {dict} 主表修改参数
        :param model_id: {int} 模型ID
        :param bk_username: {str} 用户名
        :return: {dict} 数据模型完成步骤 & 发布状态
        """
        # 1) 判断model_id对应的数据模型是否存在
        datamodel_object = get_object(DmmModelInfo, model_id=model_id)

        # 2) 获取模型相关信息
        step_id = datamodel_object.step_id
        publish_status = datamodel_object.publish_status
        model_type = datamodel_object.model_type
        table_name = datamodel_object.table_name

        # 模型主表修改前的详情,用于记录操作前的content_before_change
        orig_master_table_dict = cls.get_master_table_info(model_id, with_time_field=True)
        orig_master_table_dict.update({'model_name': datamodel_object.model_name})

        # 3) 主表校验 & 模型关联校验(按照关联维度模型的最新发布版本做校验)
        origin_fields_dict = cls.validate_master_table(
            table_name, model_type, params['fields'], params['model_relation']
        )

        # 对主表字段和模型关联进行格式化处理
        cls.format_master_table_params(model_id, params, origin_fields_dict)

        # 4) 字段修改、新增、删除
        has_update = cls.update_master_table_object(
            model_id,
            DmmModelFieldStage,
            # 发布时，用草稿态详情写db主表会返回多余字段
            params['fields'],
            bk_username,
            ['field_name'],
            origin_fields_dict,
        )

        # 5) 模型关联修改、新增、删除
        has_update |= cls.update_master_table_object(
            model_id,
            DmmModelRelationStage,
            params['model_relation'],
            bk_username,
            DATA_MODEL_UNIQUE_KEY_MAPPINGS[DataModelObjectType.MASTER_TABLE.value],
        )

        # 6）草稿态模型主表有变更时维护模型状态 & 添加操作记录
        if has_update:
            # a) 更新模型step_id&publish_status状态
            step_id, publish_status = update_data_model_state(model_id, cls.update_master_table.__name__, bk_username)
            # b) 操作记录
            master_table_dict = cls.get_master_table_info(model_id, with_time_field=True)
            master_table_dict.update({'model_name': datamodel_object.model_name})
            OperationLogManager.create_operation_log(
                created_by=bk_username,
                model_id=model_id,
                object_type=DataModelObjectType.MASTER_TABLE.value,
                object_operation=(
                    DataModelObjectOperationType.UPDATE.value
                    if orig_master_table_dict['fields']
                    else DataModelObjectOperationType.CREATE.value
                ),
                object_id=model_id,
                object_name=datamodel_object.model_name,
                object_alias=datamodel_object.model_alias,
                content_before_change=orig_master_table_dict,
                content_after_change=master_table_dict,
            )
        # 返回模型构建完成步骤
        return {'step_id': step_id, 'publish_status': publish_status}

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def sync_mst_table_from_stage_to_master(cls, model_id, params, bk_username):
        """
        模型发布同步草稿态主表(字段&模型关联)内容到db主表
        :param params: {dict} 主表修改参数
        :param model_id: {int} 模型ID
        :param bk_username: {str} 用户名
        :return:
        """
        # 1) 字段修改、新增、删除
        cls.update_master_table_object(
            model_id,
            DmmModelField,
            # 发布时，用草稿态详情写db主表会返回多余字段
            params['fields'],
            bk_username,
            ['field_name'],
        )

        # 2) 模型关联修改、新增、删除
        cls.update_master_table_object(
            model_id,
            DmmModelRelation,
            params['model_relation'],
            bk_username,
            DATA_MODEL_UNIQUE_KEY_MAPPINGS[DataModelObjectType.MASTER_TABLE.value],
        )

    @staticmethod
    def format_master_table_params(model_id, params, origin_fields_dict):
        """
        主表变更时，对主表字段和模型关联参数进行格式化处理
        :param model_id: {int} 模型ID
        :param params: {dict} 主表字典
        :param origin_fields_dict: {dict} 主表对应的SQL来源字段字典
        :return:
        """
        # 1)模型主表字段
        for field_dict in params['fields']:
            field_dict['model_id'] = model_id
            field_dict['origin_fields'] = origin_fields_dict.get(field_dict['field_name'], [])

        # 2）模型主表关联
        for model_relation_dict in params['model_relation']:
            model_relation_dict['model_id'] = model_id

    @staticmethod
    def get_model_name(model_dict):
        """获取模型英文名称

        active的模型，返回model_name；
        已经被删除的模型，去掉删除模型时给model_name加的前缀
        :param model_dict: 模型信息
        :type model_dict: dict
        :return: 模型英文名称
        :rtype: str
        """
        if 'model_name' not in model_dict or 'active_status' not in model_dict:
            return None
        model_name = model_dict['model_name']
        if model_dict['active_status'] == DataModelActiveStatus.DISABLED.value:
            model_name = model_name[DELETED_MODEL_NAME_PREFIX_LEN:]
        return model_name

    @classmethod
    def get_master_table_info(
        cls,
        model_id,
        with_time_field=False,
        allow_field_type=None,
        with_details=None,
        latest_version=False,
        origin_fields_dict=None,
    ):
        """主表详情

        :param model_id: 模型ID
        :param with_time_field: 是否显示时间字段, 默认不展示
        :type with_time_field: bool
        :param allow_field_type: 允许返回的字段数据类型, 默认全返回
        :type allow_field_type: list
        :param with_details: 展示字段是否可以被删除、被编辑等详情
        :type with_details: list
        :param latest_version: 是否返回草稿态信息，默认返回草稿态信息，为True时返回最新版本信息
        :type latest_version: bool
        :param origin_fields_dict: 主表字段对应的origin_fields mappings

        :return: 主表字段信息
        :rtype: dict
        """
        # 1) 获取主表字段列表 & 字段关联
        master_table_instance = MasterTable(model_id, with_time_field, allow_field_type, latest_version)

        # 2)设置字段是否可以删除 & 修改
        if with_details is not None and 'editable' in with_details and 'deletable' in with_details:
            master_table_instance.set_editable_deletable_info(origin_fields_dict)

        # 返回模型主表详情
        return master_table_instance.get_master_table_dict()

    @classmethod
    @auto_meta_sync(using='bkdata_basic')
    def delete_master_table(cls, model_id):
        """
        删除主表所有字典和关联关系
        :param model_id: {Int} 模型ID
        """
        # 1) 删除字段
        field_queryset = DmmModelField.objects.filter(model_id=model_id)
        for field_object in field_queryset:
            field_object.delete()
        field_stage_queryset = DmmModelFieldStage.objects.filter(model_id=model_id)
        for field_object in field_stage_queryset:
            field_object.delete()

        # 2) 删除模型关联关系
        relation_queryset = DmmModelRelation.objects.filter(model_id=model_id)
        for relation_object in relation_queryset:
            relation_object.delete()
        relation_stage_queryset = DmmModelRelationStage.objects.filter(model_id=model_id)
        for relation_object in relation_stage_queryset:
            relation_object.delete()


class MasterTable(object):
    # 模型主表：主表字段和主表关联关系
    def __init__(self, model_id, with_time_field=False, allow_field_type=None, latest_version=False):
        self.model_id = model_id
        # 当前模型对应的DmmModelInfo object
        self.datamodel_object = None
        self.set_datamodel_object()

        # 是否返回最新版本
        self.latest_version = latest_version
        # 是否返回时间字段
        self.with_time_field = with_time_field
        # 允许返回的数据类型列表
        self.allow_field_type = allow_field_type

        # 模型关联对应的models class
        self.model_relation_cls = DmmModelRelation if self.latest_version else DmmModelRelationStage
        # 模型字段对应的models class
        self.model_field_cls = DmmModelField if self.latest_version else DmmModelFieldStage

        # 主表关联
        self.relations = []
        # 关联维度模型中被关联字段信息
        self._related_field_dict = None
        # 关联维度模型信息
        self._related_model_dict = None
        # 关联维度模型model_ids
        self._related_model_ids = None
        # 主表中关联字段信息
        self._join_field_dict = None
        self.set_relations()
        self.validate_model_relation()
        self.set_related_model_attrs()

        # 主表字段
        self.fields = []
        self.set_fields()
        # 调整主表字段顺序
        self.change_fields_sequence()

        # 主表字段被主表内其他字段/统计口径/指标使用
        # 字段被主表内其他字段加工逻辑引用
        self.field_used_by_other_fields_dict = {}
        # 模型创建和引用的统计口径object列表
        self._calculation_atom_object_list = None
        # 字段被统计口径的聚合逻辑引用
        self.field_used_by_calc_atom_dict = {}
        # 模型的指标object列表
        self._indicator_object_list = None
        # 字段被指标作为聚合字段
        self.aggregation_field_dict = {}
        # 字段被指标作为过滤字段
        self.condition_field_dict = {}

    def set_relations(self):
        """
        设置模型关联
        :return:
        """
        # 模型关联列表
        self.relations = [
            model_to_dict(relation_object, exclude=['created_by', 'updated_by'])
            for relation_object in self.model_relation_cls.objects.filter(model_id=self.model_id)
        ]

    @property
    def related_model_ids(self):
        """
        被关联的模型model_ids
        :return:
        """
        if self._related_model_ids is None:
            self._related_model_ids = [relation_dict['related_model_id'] for relation_dict in self.relations]
        return self._related_model_ids

    @property
    def related_field_dict(self):
        """
        关联维度模型中被关联字段信息{主表中的关联字段: 被关联维度字段信息}
        :return:
        """
        if self._related_field_dict is None:
            self._related_field_dict = {
                relation_dict['field_name']: {
                    'model_id': relation_dict['related_model_id'],
                    'field_name': relation_dict['related_field_name'],
                }
                for relation_dict in self.relations
            }
        return self._related_field_dict

    @property
    def join_field_dict(self):
        """
        主表中关联字段信息{被关联模型id: 主表中关联字段field_name}
        :return:
        """
        if self._join_field_dict is None:
            self._join_field_dict = {
                relation_dict['related_model_id']: relation_dict['field_name'] for relation_dict in self.relations
            }
        return self._join_field_dict

    @property
    def related_model_dict(self):
        """
        被关联维度模型信息{被关联维度模型id: 被关联维度模型信息}
        :return:
        """
        if self._related_model_dict is None:
            self._related_model_dict = {
                related_model_obj.model_id: models_object_to_dict(related_model_obj)
                for related_model_obj in DmmModelInfo.origin_objects.filter(model_id__in=self.related_model_ids)
            }
        return self._related_model_dict

    def set_related_model_attrs(self):
        """
        设置被关联维度模型的英文名称和active_status
        :return:
        """
        # 被关联维度模型的英文名称和active_status
        for relation_dict in self.relations:
            relation_dict['related_model_name'] = MasterTableManager.get_model_name(
                self.related_model_dict[relation_dict['related_model_id']]
            )
            relation_dict['related_model_active_status'] = self.related_model_dict[relation_dict['related_model_id']][
                'active_status'
            ]

    def validate_model_relation(self):
        """
        校验被关联的维度模型是否存在
        :return:
        """
        added_related_model_ids, deleted_related_model_ids, same_related_model_ids = list_diff(
            self.related_model_ids, list(self.related_model_dict.keys())
        )
        if deleted_related_model_ids:
            raise dm_pro_errors.DataModelNotExistError(
                message=_('关联的维度模型不存在, model_ids:{}'.format(deleted_related_model_ids))
            )

    def set_fields(self):
        """
        设置主表字段列表
        :return:
        """
        field_queryset = self.model_field_cls.objects.filter(model_id=self.model_id).order_by('field_index')
        for field_object in field_queryset:
            # 默认不展示时间字段
            if not self.with_time_field:
                if field_object.field_name == TimeField.TIME_FIELD_NAME:
                    continue
            # 只返回允许数据类型的字段，默认全部返回
            if self.allow_field_type:
                if field_object.field_type not in self.allow_field_type:
                    continue
            self.fields.append(self.field_object_to_dict(field_object))

    def field_object_to_dict(self, field_object):
        """
        将主表字段object转化为dict
        :param field_object: {dict} 主表字段object
        :return: field_dict:{dict} 主表字段信息
        """
        field_dict = model_to_dict(field_object, exclude=['created_by', 'updated_by'])
        field_dict['is_join_field'] = False
        field_dict['is_extended_field'] = False
        field_dict['join_field_name'] = None

        # 是否主表关联字段
        if field_dict['field_name'] in self.related_field_dict:
            field_dict['is_join_field'] = True
            field_dict['related_field_exist'] = True
            try:
                # 获取被关联维度表中的被关联字段
                get_object(DmmModelField, **(self.related_field_dict[field_dict['field_name']]))
            except dm_pro_errors.FieldNotExistError:
                logger.error(
                    'related_field of join field does not exist, related_field_info:{}'.format(
                        self.related_field_dict[field_dict['field_name']]
                    )
                )
                field_dict['related_field_exist'] = False

        # 扩展字段, 获取对应的主表关联字段, 对应的字段约束和加工逻辑
        elif field_dict['source_model_id'] and field_dict['source_field_name']:
            self.set_extended_field_attr(field_dict)
        return field_dict

    def change_fields_sequence(self):
        """
        调整字段顺序
        :return: ret_field_list: {List}
        """
        # 1) 将非拓展字段按照顺序放置field_list
        field_list = []
        join_extended_field_map = {}
        for field_dict in self.fields:
            if not field_dict['is_extended_field']:
                field_list.append(field_dict)
            else:
                if field_dict['join_field_name'] and field_dict['join_field_name'] not in join_extended_field_map:
                    join_extended_field_map[field_dict['join_field_name']] = []
                join_extended_field_map[field_dict['join_field_name']].append(field_dict)

        # 2) 按照用户顺序将拓展字段放在关联字段后面,并修改field_index
        ret_field_list = []
        field_index = 1
        for field_dict in field_list:
            ret_field_list.append(field_dict)
            field_dict['field_index'] = field_index
            field_index += 1
            if field_dict['is_join_field']:
                for extended_field_dict in join_extended_field_map.get(field_dict['field_name'], []):
                    ret_field_list.append(extended_field_dict)
                    extended_field_dict['field_index'] = field_index
                    field_index += 1
        self.fields = ret_field_list

    def set_extended_field_attr(self, field_dict):
        """
        设置主表中拓展字段的属性: 主表关联字段、约束内容、加工逻辑、数据类型、字段角色
        :param field_dict: {Dict} 主表拓展字段内容，例如{'field_name':'xx', 'field_type': 'dimension'}
        :return:
        """
        # 是否扩展字段
        field_dict['is_extended_field'] = True
        # 对应的主表关联字段
        field_dict['join_field_name'] = self.join_field_dict[field_dict['source_model_id']]

        # 扩展字段的字段约束/加工逻辑/数据类型/字段角色
        field_dict['field_constraint_content'] = None
        field_dict['field_clean_content'] = None
        field_dict['field_type'] = None
        field_dict['field_category'] = None
        field_dict['source_field_exist'] = False

        # 关联模型被删除：source_model_id对应的active_status是disabled
        if (
            self.related_model_dict[field_dict['source_model_id']]['active_status']
            == DataModelActiveStatus.DISABLED.value
        ):
            logger.error('source_model has been deleted, source_model_id:{}'.format(field_dict['source_model_id']))
            return

        # 获取维度模型中对应的扩展字段
        try:
            source_field_object = get_object(
                DmmModelField, model_id=field_dict['source_model_id'], field_name=field_dict['source_field_name']
            )
        # 关联模型存在，扩展字段被删除：关联模型中的source_field_name不存在
        except dm_pro_errors.FieldNotExistError:
            logger.error(
                'source_field:{} of extended field:{} does not exist, source_model_id:{}'.format(
                    field_dict['source_field_name'], field_dict['field_name'], field_dict['source_model_id']
                )
            )
            return

        # 扩展字段存在：被关联模型中的source_field_name存在，设置扩展字段的字段约束/加工逻辑/数据类型/字段角色
        field_dict['source_field_exist'] = True
        field_dict['field_constraint_content'] = source_field_object.field_constraint_content
        field_dict['field_clean_content'] = source_field_object.field_clean_content
        field_dict['field_type'] = source_field_object.field_type
        field_dict['field_category'] = source_field_object.field_category

    def set_datamodel_object(self):
        """
        设置当前模型对应的DmmModelInfo object
        """
        self.datamodel_object = get_object(DmmModelInfo, model_id=self.model_id)

    def set_field_used_by_other_fields_dict(self, origin_fields_dict=None):
        """设置字段被主表内其他字段加工逻辑引用

        :param origin_fields_dict: 主表字段对应的origin_fields mappings

        :return:
        """
        for field_dict in self.fields:
            if origin_fields_dict:
                field_dict['origin_fields'] = origin_fields_dict.get(field_dict['field_name'], [])
            for field_name in field_dict['origin_fields']:
                if field_name != field_dict['field_name']:
                    if field_name not in self.field_used_by_other_fields_dict:
                        self.field_used_by_other_fields_dict[field_name] = []
                    self.field_used_by_other_fields_dict[field_name].append({'field_name': field_dict['field_name']})

    def set_field_used_by_calc_atom_dict(self):
        """
        设置字段被统计口径聚合逻辑引用字典（字段被统计口径的聚合逻辑引用）
        :return:
        """
        for calc_atom_obj in self.calculation_atom_object_list:
            for origin_field in calc_atom_obj.origin_fields:
                if origin_field not in self.field_used_by_calc_atom_dict:
                    self.field_used_by_calc_atom_dict[origin_field] = []
                self.field_used_by_calc_atom_dict[origin_field].append(
                    {
                        'calculation_atom_name': calc_atom_obj.calculation_atom_name,
                        'calculation_atom_alias': calc_atom_obj.calculation_atom_alias,
                    }
                )

    @property
    def calculation_atom_object_list(self):
        """
        模型创建和引用的统计口径object列表
        :return: {[Object]} 模型创建和引用的统计口径object列表
        """
        if self._calculation_atom_object_list is None:
            self._calculation_atom_object_list = CalculationAtomManager.get_calculation_atom_object_list(self.model_id)
        return self._calculation_atom_object_list

    def set_aggregation_field_dict(self):
        """
        设置聚合字段字典（字段被指标作为聚合字段）
        :return:
        """
        for indicator_obj in self.indicator_object_list:
            for aggregation_field in indicator_obj.aggregation_fields:
                if aggregation_field not in self.aggregation_field_dict:
                    self.aggregation_field_dict[aggregation_field] = []
                self.aggregation_field_dict[aggregation_field].append(
                    {'indicator_name': indicator_obj.indicator_name, 'indicator_alias': indicator_obj.indicator_alias}
                )

    def set_condition_field_dict(self):
        """
        设置过滤字段字典（字段被指标作为过滤字段）
        :return:
        """
        for indicator_obj in self.indicator_object_list:
            for condition_field in indicator_obj.condition_fields:
                if condition_field not in self.condition_field_dict:
                    self.condition_field_dict[condition_field] = []
                self.condition_field_dict[condition_field].append(
                    {'indicator_name': indicator_obj.indicator_name, 'indicator_alias': indicator_obj.indicator_alias}
                )

    @property
    def indicator_object_list(self):
        """
        当前模型对应的指标object列表
        :return: {[Object]} 模型对应的指标object列表
        """
        if self._indicator_object_list is None:
            self._indicator_object_list = DmmModelIndicatorStage.objects.filter(model_id=self.model_id)
        return list(self._indicator_object_list)

    def set_is_join_field(self, field_dict):
        """
        设置字段是否是关联字段
        :param field_dict: {Dict} 字段详情
        :return:
        """
        field_dict['editable_deletable_info'] = {'is_join_field': field_dict['is_join_field']}

    def set_is_used_by_other_fields(self, field_dict):
        """设置字段是否被主表内其他字段加工逻辑引用

        :param field_dict: 字段详情
        :return:
        """
        if field_dict['field_name'] in self.field_used_by_other_fields_dict:
            field_dict['editable_deletable_info'].update(
                {
                    'is_used_by_other_fields': True,
                    'fields': self.field_used_by_other_fields_dict[field_dict['field_name']],
                }
            )
        else:
            field_dict['editable_deletable_info']['is_used_by_other_fields'] = False

    def set_is_used_by_calculation_atom(self, field_dict):
        """
        设置字段是否被统计口径聚合逻辑引用字典
        :param field_dict: {Dict} 字段详情
        :return:
        """
        if field_dict['field_name'] in self.field_used_by_calc_atom_dict:
            field_dict['editable_deletable_info'].update(
                {
                    'is_used_by_calc_atom': True,
                    'calculation_atoms': self.field_used_by_calc_atom_dict[field_dict['field_name']],
                }
            )
        else:
            field_dict['editable_deletable_info']['is_used_by_calc_atom'] = False

    def set_is_aggregation_field(self, field_dict):
        """
        设置字段是否被指标作为聚合字段
        :param field_dict: {Dict} 字段详情
        :return:
        """
        if field_dict['field_name'] in self.aggregation_field_dict:
            field_dict['editable_deletable_info'].update(
                {
                    'is_aggregation_field': True,
                    'aggregation_field_indicators': self.aggregation_field_dict[field_dict['field_name']],
                }
            )
        else:
            field_dict['editable_deletable_info']['is_aggregation_field'] = False

    def set_is_condition_field(self, field_dict):
        """
        设置字段是否被指标作为过滤字段
        :param field_dict: {Dict} 字段详情
        :return:
        """
        if field_dict['field_name'] in self.condition_field_dict:
            field_dict['editable_deletable_info'].update(
                {
                    'is_condition_field': True,
                    'condition_field_indicators': self.condition_field_dict[field_dict['field_name']],
                }
            )
        else:
            field_dict['editable_deletable_info']['is_condition_field'] = False

    def set_deletable(self, field_dict):
        """
        设置字段是否可以被删除
        :param field_dict: {Dict} 字段详情
        :return:
        """
        if (
            field_dict['editable_deletable_info']['is_used_by_other_fields']
            or field_dict['editable_deletable_info']['is_used_by_calc_atom']
            or field_dict['editable_deletable_info']['is_aggregation_field']
            or field_dict['editable_deletable_info']['is_condition_field']
        ):
            field_dict['deletable'] = False
        else:
            field_dict['deletable'] = True

    def set_editable(self, field_dict):
        """
        设置字段是否可以被编辑
        :param field_dict: {Dict} 字段详情
        :return:
        """
        if not field_dict['deletable'] or field_dict['editable_deletable_info']['is_join_field']:
            field_dict['editable'] = False
        else:
            field_dict['editable'] = True
        if field_dict['editable_deletable_info']['is_used_by_calc_atom']:
            field_dict['editable_field_type'] = False
        else:
            field_dict['editable_field_type'] = True

    def set_join_field_deletable(self):
        """
        设置关联字段能否删除（扩展字段不能删除，对应的关联字段也不能删除）
        :return:
        """
        join_fields_not_deletable_editable = list(
            set(
                [
                    field_dict['join_field_name']
                    for field_dict in self.fields
                    if field_dict['is_extended_field'] and not field_dict['deletable']
                ]
            )
        )

        for field_dict in self.fields:
            if field_dict['is_join_field'] and field_dict['field_name'] in join_fields_not_deletable_editable:
                field_dict['deletable'] = False
                field_dict['editable'] = False
                field_dict['editable_deletable_info']['is_extended_field_deletable_editable'] = False

    def set_editable_deletable_info(self, origin_fields_dict=None):
        """设置主表字段是否可以编辑&删除

        :param origin_fields_dict: 主表字段对应的origin_fields mappings

        :return:
        """
        self.set_field_used_by_other_fields_dict(origin_fields_dict)
        self.set_field_used_by_calc_atom_dict()
        self.set_aggregation_field_dict()
        self.set_condition_field_dict()
        for field_dict in self.fields:
            self.set_is_join_field(field_dict)
            self.set_is_used_by_other_fields(field_dict)
            self.set_is_used_by_calculation_atom(field_dict)
            self.set_is_aggregation_field(field_dict)
            self.set_is_condition_field(field_dict)
            self.set_deletable(field_dict)
            self.set_editable(field_dict)
        self.set_join_field_deletable()

    def get_master_table_dict(self):
        """
        获取主表详情
        :return:
        """
        return {
            'model_id': self.model_id,
            'fields': self.fields,
            'model_relation': self.relations,
            'step_id': self.datamodel_object.step_id,
            'publish_status': self.datamodel_object.publish_status,
        }
