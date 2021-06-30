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

import attr
from auth.exceptions import (
    NotObjectClassSameWithMeta,
    ObjectNotExistsErr,
    ObjectSerilizerNoPKErr,
)
from auth.handlers.lineage_search import LineageHandler
from auth.models.auth_models import ProjectData
from auth.models.base_models import ActionConfig
from auth.models.outer_models import (
    PROCESSING_TYPE,
    AccessRawData,
    AlgoModel,
    Algorithm,
    AlgoSampleSet,
    BusinessInfo,
    DashboardInfo,
    DataModel,
    DataTokenInfo,
    FlowInfo,
    FunctionInfo,
    ProjectInfo,
    ResourceGroupInfo,
    ResultTable,
)
from auth.templates.template_manage import TemplateManager
from common.log import logger

GLOBAL_SCOPE = {"*": "*"}


def is_sys_scopes(scopes):
    for _scope in scopes:
        if _scope == GLOBAL_SCOPE:
            return True

    return False


class SerializerMixin:
    @classmethod
    def clean(cls, data):
        """
        用过 attrs 的检验方法，校验并装换 data 数据

        @param {Dict} data
        """
        cleaned_data = {}
        for f in attr.fields(cls):
            if f.name in data:
                cleaned_data[f.name] = f.type(data[f.name])

        return cleaned_data


class BaseBKDataObject:
    @attr.s
    class _Serializer(SerializerMixin):
        pass

    # to be implemented in subclass
    auth_object_class = ""
    # Object 主键名称
    input_val_key = None
    outer_model = None
    parent_models = []

    metadata_dgraph_table = ""
    metadata_type = ""

    @classmethod
    def init_by_object(cls, obj):
        instance = cls(object_id=obj[cls.input_val_key])
        instance._obj = obj

        return instance

    def __init__(self, object_id=None):
        self.object_id = self.clean_primary_key(object_id)
        self._obj = None

    def is_in_scope(self, scope, action_id=None):
        """
        对象需要具备判断 scope 包含关系，函数入口
        """
        scope = self.clean_scopes(scope)

        if self.is_sys_scope(scope):
            return True

        if self.is_in_self_scope(scope, action_id=action_id):
            return True

        return False

    def is_in_self_scope(self, scope, action_id=None):
        """
        对象自身判断 scope 包含关系，需要子类继承，实现逻辑
        """
        if len(scope) == 0:
            return False

        ret = True
        for scope_key, scope_val in list(scope.items()):
            direct_scopes = self.retrieve_direct_scopes(scope_key)
            if scope_val in direct_scopes:
                continue

            indirect_scopes = self.retrieve_indirect_scopes(scope_key, action_id)
            if scope_val in indirect_scopes:
                continue

            ret = False
            break

        return ret

    def retrieve_scope_ids(self, object_class_id, action_id, is_direct=True):
        """
        获取实例的关联范围
        """
        if object_class_id not in ObjectFactory.BKDATA_OBJECTS:
            return []

        primary_key = ObjectFactory.BKDATA_OBJECTS[object_class_id].input_val_key
        if is_direct:
            return self.retrieve_direct_scopes(primary_key)
        else:
            return self.retrieve_indirect_scopes(primary_key, action_id)

    def retrieve_direct_scopes(self, attr_key, only_id=True):
        """
        获取实例直接关联的属性范围，举例说明

        实例 A(project_id=1, bk_biz_id=3, result_table_id=3_xxx) 关于 bk_biz_id 属性范围为 (bk_biz_id=3)
        """
        if attr_key == "*":
            return ["*"]

        # 若是主键范围，则无需多查一次 obj 整个对象内容
        if attr_key == self.input_val_key:
            return [self.object_id]

        if hasattr(self.obj, attr_key):
            attr_value = [getattr(self.obj, attr_key)]
        else:
            attr_value = []

        if only_id:
            return attr_value

        return {attr_key: attr_value}

    def retrieve_indirect_scopes(self, attr_key, action_id=None, only_id=True):
        """
        钩子函数，获取实例非直接关联的属性范围，非通用逻辑，子类根据实际情况进行重载
        """
        return [] if only_id else {attr_key: []}

    @classmethod
    def to_graph_statement(cls, scopes, variable_name):
        """
        将用户有权限范围转换为 dgraph 语句，并以 $variable_name 命名
        """
        compressed_scopes = cls.get_compressed_scopes(scopes)

        # 默认规则，如果是全局，则以星号告知 dgraph，无需过滤
        if is_sys_scopes(compressed_scopes):
            return "*"

        # 如果没有权限，则返回空字符
        if len(compressed_scopes) == 0:
            return ""

        template = TemplateManager().get_template("/dgraph/auth_scopes.mako")
        # 如果属性范围存在无法装换为 dgraph 字段的情况，暂时先忽略
        map_auth_attr = {
            f.name: f.metadata["dgraph_field"] for f in attr.fields(cls._Serializer) if "dgraph_field" in f.metadata
        }
        filters = [
            [(map_auth_attr[_k], json.dumps(_v)) for _k, _v in list(scope.items())]
            for scope in compressed_scopes
            if all([key in map_auth_attr for key in scope])
        ]

        statement = template.render(
            variable=variable_name, dgraph_table=cls.metadata_dgraph_table, dgraph_filters=filters
        )
        return statement.replace("\n", " ")

    @classmethod
    def get_related_scopes(cls, scopes, action_id=None, show_admin_scopes=False):
        """
        遍历得到所有关联的对象列表，为了兼容，需要返回原始 scope 结构

        @example 装换例子
            [
                {'bk_biz_id': 591, 'sensitivity': 'private'},
                {'result_table_id': '599_rt'}
            ]
                                |
                                V
            [
                {'bk_biz_id': 591, 'sensitivity': 'private'},   # 后续期望不返回原始结构
                {'result_table_id': '599_rt'},
                {'result_table_id': '591_rt1'},
                {'result_table_id': '591_rt2'}
            ]
        """
        primary_key = cls.input_val_key
        related_scopes = [_s for _s in scopes if primary_key in _s]

        compressed_scopes = cls.get_compressed_scopes(scopes)
        # 如果是超级范围，暂时默认不返回全部数据
        if is_sys_scopes(compressed_scopes) and not show_admin_scopes:
            related_scopes.extend(compressed_scopes)
            return related_scopes

        for compressed_scope in compressed_scopes:
            if primary_key not in compressed_scope:
                related_scopes.extend(cls.get_direct_instance_scopes(compressed_scope))
                related_scopes.extend(cls.get_indirect_instance_scopes(compressed_scope, action_id=action_id))

        return cls.remove_duplication(related_scopes)

    @classmethod
    def get_compressed_scopes(cls, scopes):
        """
        对范围进行压缩，如果有超级范围，则直接返回 [GLOBAL_SCOPE]，若没有则分别对单属性和多属性范围进行压缩
        """
        compressed_scopes = []

        if is_sys_scopes(scopes):
            return [GLOBAL_SCOPE]

        one_attr_scopes = list()
        more_attr_scopes = list()
        for scope in scopes:
            # 空范围
            if len(scope) == 0:
                continue
            # 单一属性范围
            elif len(scope) == 1:
                one_attr_scopes.append(scope)
            # 多属性范围
            else:
                more_attr_scopes.append(scope)

        compressed_scopes.extend(cls.one_attr_compress(one_attr_scopes))
        compressed_scopes.extend(cls.more_attr_compress(more_attr_scopes))

        return compressed_scopes

    @classmethod
    def one_attr_compress(cls, scopes):
        """
        单属性范围压缩
        @example
            [
                {'project_id': 111},
                {'project_id': 112},
                {'project_id': 113}
            ]

            ->

            [
                {'project_id': [111, 112, 113]}
            ]
        """
        one_attr_mapping = dict()
        for scope in scopes:
            _k = list(scope.keys())[0]
            _v = list(scope.values())[0]
            if _k not in one_attr_mapping:
                one_attr_mapping[_k] = [_v]
            else:
                one_attr_mapping[_k].append(_v)

        return [{_field: _content} for _field, _content in list(one_attr_mapping.items())]

    @classmethod
    def more_attr_compress(cls, scopes):
        """
        多属性范围压缩，便于后续的过滤条件的组合

        @example
            [
                {'bk_biz_id': 111, 'sensitivity': 'private'},
                {'bk_biz_id': 112, 'sensitivity': 'private'},
                {'bk_biz_id': 113, 'sensitivity': 'confidential'}
            ]
                            |
                            V
            [
                {'sensitivity': 'private', 'bk_biz_id': ['111', '112']},
                {'sensitivity': 'confidential', 'bk_biz_id': ['113']}
            ]

        """
        dimension_fields = []
        for f in attr.fields(cls._Serializer):
            if f.metadata.get("is_dimension", False):
                dimension_fields.append(f.name)

        """
        尝试构件易于组装的结构
            {
                'sensitivity::bk_biz_id': {
                    ('private'): ['111', '112'],
                    ('confidential'): ['113']
                }
            }
        """
        mapping = dict()
        for scope in scopes:
            dim_fields = [_k for _k in list(scope.keys()) if _k in dimension_fields]
            val_fields = [_k for _k in list(scope.keys()) if _k not in dimension_fields]

            # 单数据指标才支持压缩
            if not (len(val_fields) == 1 and len(dim_fields) > 0):
                continue

            index = "{}::{}".format(",".join(dim_fields), val_fields[0])
            if index not in mapping:
                mapping[index] = dict()

            dim_fields_content = tuple(scope[f] for f in dim_fields)
            val_fields_content = scope[val_fields[0]]

            if dim_fields_content not in mapping[index]:
                mapping[index][dim_fields_content] = [val_fields_content]
            else:
                mapping[index][dim_fields_content].append(val_fields_content)

        compressed_scopes = []
        for index, content in list(mapping.items()):

            dim_fields = index.split("::")[0].split(",")
            val_field = index.split("::")[1]
            for dim_fields_content, val_fields_content_arr in list(content.items()):
                _scope = dict()
                _scope.update(list(zip(dim_fields, dim_fields_content)))
                _scope[val_field] = val_fields_content_arr

                compressed_scopes.append(_scope)

        return compressed_scopes

    @classmethod
    def get_direct_instance_scopes(cls, scope):
        """
        根据 scope 组装 model 查询语句，返回对象列表
        """
        if cls.is_sys_scope(scope):
            return cls.outer_model.objects.values(cls.input_val_key)

        # 如果出现不在实例表的字段，则忽略
        outer_model_fields = [f.name for f in cls.outer_model._meta.fields]
        for key in list(scope.keys()):
            if key not in outer_model_fields:
                return []
        try:
            filters = {f"{k}__in" if type(v) == list else k: v for k, v in list(scope.items())}
            return cls.outer_model.objects.filter(**filters).values(cls.input_val_key)
        except Exception as err:
            logger.exception(
                "[ObjectClassScope] Fail to get {object_class}.direct_instance_scopes, {scope}, "
                "{err}".format(object_class=cls.auth_object_class, scope=scope, err=err)
            )
            return []

    @classmethod
    def get_indirect_instance_scopes(cls, scope, action_id=None):
        """
        根据 scope 查询非直接关联的实例列表，给子类预留的钩子函数
        """
        return []

    @classmethod
    def clean_scopes(cls, scope):
        """
        对于系统解析出来的范围进行清洗
        """
        if cls.is_sys_scope(scope):
            return GLOBAL_SCOPE

        return cls._Serializer.clean(data=scope)

    @classmethod
    def clean_primary_key(cls, value):
        """
        根据主键类型，对传入数值进行清洗和类型转换
        """
        for f in attr.fields(cls._Serializer):
            if f.name == cls.input_val_key:
                return f.type(value)

        raise ObjectSerilizerNoPKErr()

    @classmethod
    def wrap_display(
        cls, data, input_val_key=None, output_val_key=None, display_key_maps=None, key_path=None, filters=None
    ):
        """
        补充显示名称
        @param data:
        @param input_val_key: 输入值的键
        @param output_val_key: 输出值的键
        @param display_key_maps: 其它替换字段
        @param key_path: 输入字段所在的字典路径
        @param filters: 过滤条件
        @return:
        """
        if input_val_key is None:
            input_val_key = cls.input_val_key
        return cls.outer_model.wrap_display(
            data,
            input_val_key,
            output_val_key=output_val_key,
            display_key_maps=display_key_maps,
            key_path=key_path,
            filters=filters,
        )

    @classmethod
    def list(cls, input_val_key=None, output_val_key=None, display_key_maps=None, key_path=None, filters=None):
        """
        资源列表, 参数参考wrap_display，主要目的是为了能够自定义字段名和数据结构
        @return:
        """
        filters = cls.clean_filters(filters)

        data = cls.outer_model.list(filters=filters)
        return cls.wrap_display(
            data,
            input_val_key=input_val_key,
            output_val_key=output_val_key,
            display_key_maps=display_key_maps,
            key_path=key_path,
            filters=filters,
        )

    @classmethod
    def clean_filters(cls, filters=None):
        """
        清洗过滤条件，只允许配置的过滤字段
        @return:
        """
        if filters is None:
            return {}
        return {_key: _value for _key, _value in list(filters.items()) if _key in cls.outer_model.allowed_filter_keys}

    @staticmethod
    def is_sys_scope(scope):
        """
        判断 scope 是否为系统范围，目前系统范围最大，可包含任意范围
        """
        return scope == GLOBAL_SCOPE

    def __getattr__(self, name):
        """
        属性拦截器，比如 obj__bk_biz_id，获取对象的 bk_biz_id 字段
        """
        # 拦截访问内部对象的属性
        _object_prefix = "obj__"

        if name.startswith(_object_prefix):
            _actural_name = name[len(_object_prefix) :]
            return getattr(self.obj, _actural_name)

        # 默认行为
        raise AttributeError

    @property
    def obj(self):
        if self._obj is None:
            _obj = self.search_object()
            if _obj is None:
                raise ObjectNotExistsErr()

            self._obj = _obj

        return self._obj

    def search_object(self):
        """
        获取实例对象方法，子类继承实现，如果找不到则返回 None
        """
        pass

    @staticmethod
    def remove_duplication(scopes):
        """
        对 Scopes 去重
        """
        return [dict(_scope_tuple) for _scope_tuple in {tuple(scope.items()) for scope in scopes}]


class AuthBusiness(BaseBKDataObject):
    auth_object_class = "biz"
    input_val_key = "bk_biz_id"
    outer_model = BusinessInfo

    metadata_dgraph_table = "BKBiz"
    metadata_type = "bk_biz"

    @attr.s
    class _Serializer(SerializerMixin):
        bk_biz_id = attr.ib(type=int, metadata={"dgraph_field": "BKBiz.id"})

    def __init__(self, object_id=None):
        super().__init__(object_id=object_id)


class AuthProject(BaseBKDataObject):
    auth_object_class = "project"
    input_val_key = "project_id"
    outer_model = ProjectInfo

    metadata_dgraph_table = "ProjectInfo"
    metadata_type = "project_info"

    @attr.s
    class _Serializer(SerializerMixin):
        project_id = attr.ib(type=int, metadata={"dgraph_field": "ProjectInfo.project_id"})

    def __init__(self, object_id=None):
        super().__init__(object_id=object_id)


class AuthFlow(BaseBKDataObject):
    auth_object_class = "flow"
    input_val_key = "flow_id"
    outer_model = FlowInfo
    parent_models = [ProjectInfo]

    @attr.s
    class _Serializer(SerializerMixin):
        project_id = attr.ib(type=int)
        flow_id = attr.ib(type=int)

    def __init__(self, object_id=None):
        super().__init__(object_id=object_id)

    def search_object(self):
        try:
            return FlowInfo.objects.get(flow_id=self.object_id)
        except (FlowInfo.DoesNotExist, ValueError):
            pass

        return None


class AuthResultTable(BaseBKDataObject):
    auth_object_class = "result_table"
    input_val_key = "result_table_id"
    outer_model = ResultTable
    parent_models = [BusinessInfo, ProjectInfo, ProjectData]

    metadata_dgraph_table = "ResultTable"
    metadata_type = "result_table"

    @attr.s
    class _Serializer(SerializerMixin):
        project_id = attr.ib(type=int, metadata={"dgraph_field": "ResultTable.project_id"})
        bk_biz_id = attr.ib(type=int, metadata={"dgraph_field": "ResultTable.bk_biz_id"})
        result_table_id = attr.ib(type=str, metadata={"dgraph_field": "ResultTable.result_table_id"})
        sensitivity = attr.ib(type=str, metadata={"dgraph_field": "ResultTable.sensitivity", "is_dimension": True})
        processing_type = attr.ib(
            type=str, metadata={"dgraph_field": "ResultTable.processing_type", "is_dimension": True}
        )
        raw_data_id = attr.ib(type=int)

    def __init__(self, object_id=None, project_id=None):
        """
        project_id 可以废弃，待调用部分去掉可移除
        """
        super().__init__(object_id=object_id)
        self._indirect_project_ids = None
        self._indirect_raw_data_ids = None

    def search_object(self):
        try:
            return ResultTable.objects.get(result_table_id=self.object_id)
        except (ResultTable.DoesNotExist, ValueError):
            pass

        return None

    @classmethod
    def get_indirect_instance_scopes(cls, scope, action_id=None):
        if len(scope) == 1 and "project_id" in scope:
            if action_id in ["result_table.query_data", "result_table.retrieve"]:
                project_ids = scope["project_id"]
                return list(ProjectData.objects.filter(project_id__in=project_ids).values("result_table_id"))

        if len(scope) == 1 and "raw_data_id" in scope:
            raw_data_ids = scope["raw_data_id"]
            ids = AccessRawData.list_etl_result_table_ids_batch(raw_data_ids)
            return [{"result_table_id": _id} for _id in ids]

        return []

    def retrieve_indirect_scopes(self, attr_key, action_id=None, only_id=True):
        """
        获取非直接关联的 project_id 范围
        """
        ids = []
        if attr_key == "project_id" and action_id in ["result_table.query_data", "result_table.retrieve"]:
            ids = self.indirect_project_ids
        elif attr_key == "raw_data_id":
            ids = self.indirect_raw_data_ids

        if only_id:
            return ids

        return {attr_key: ids}

    @property
    def indirect_raw_data_ids(self):
        if self._indirect_raw_data_ids is None:
            if self.obj.processing_type == PROCESSING_TYPE.CLEAN:
                try:
                    self._indirect_raw_data_ids = [self.obj.get_parent_raw_data_id()]
                except Exception as err:
                    logger.warning(
                        "[ObjectClassAttr] Fail to get {object_class}.{object_id}.direct_raw_data_id, "
                        "{err}".format(object_class=self.auth_object_class, object_id=self.object_id, err=err)
                    )
            if self._indirect_raw_data_ids is None:
                self._indirect_raw_data_ids = []

        return self._indirect_raw_data_ids

    @property
    def indirect_project_ids(self):
        """
        获取 RT 关联的项目 ID 列表，即已经获取授权的关系
        """
        if self._indirect_project_ids is None:
            self._indirect_project_ids = list(
                ProjectData.objects.filter(result_table_id=self.object_id, active=True).values_list(
                    "project_id", flat=True
                )
            )

        return self._indirect_project_ids

    @property
    def bk_biz_id(self):
        return self.obj.bk_biz_id

    def get_parent_project(self):
        """
        获取 RT 所属项目
        @todo 后续查找关联对象，需要统一规范和主框架
        """
        return AuthProject(self.obj.project_id)

    def get_parent_raw_data(self):
        """
        获取 RT 所属 raw_data，目前仅清洗表支持追溯 raw_data
        """
        return AuthRawData(self.obj.get_parent_raw_data_id())

    def search_raw_data_list(self, only_id=True):
        """
        通过血缘获取 raw_data 列表
        """
        arr = LineageHandler.search_input_raw_data_arr(self.object_id)
        if only_id:
            return [raw_data["raw_data_id"] for raw_data in arr]

        return [AuthRawData(object_id=raw_data["raw_data_id"]) for raw_data in arr]


class AuthRawData(BaseBKDataObject):
    auth_object_class = "raw_data"
    input_val_key = "raw_data_id"
    outer_model = AccessRawData
    parent_models = [BusinessInfo]

    metadata_dgraph_table = "AccessRawData"
    metadata_type = "access_raw_data"

    @attr.s
    class _Serializer(SerializerMixin):
        bk_biz_id = attr.ib(type=int, metadata={"dgraph_field": "AccessRawData.bk_biz_id"})
        raw_data_id = attr.ib(type=int, metadata={"dgraph_field": "AccessRawData.id"})
        sensitivity = attr.ib(type=str, metadata={"dgraph_field": "AccessRawData.sensitivity", "is_dimension": True})

    def __init__(self, object_id=None):
        super().__init__(object_id=object_id)

    def search_object(self):
        try:
            return AccessRawData.objects.get(id=self.object_id)
        except (AccessRawData.DoesNotExist, ValueError):
            pass

        return None


class AuthDashboard(BaseBKDataObject):
    auth_object_class = "dashboard"
    input_val_key = "dashboard_id"
    outer_model = DashboardInfo
    parent_models = [ProjectInfo]

    @attr.s
    class _Serializer(SerializerMixin):
        project_id = attr.ib(type=int)
        dashboard_id = attr.ib(type=int)

    def __init__(self, object_id=None):
        super().__init__(object_id=object_id)

    def search_object(self):
        try:
            return DashboardInfo.objects.get(dashboard_id=self.object_id)
        except (DashboardInfo.DoesNotExist, ValueError):
            pass

        return None


class AuthFunction(BaseBKDataObject):
    auth_object_class = "function"
    input_val_key = "function_id"
    outer_model = FunctionInfo

    @attr.s
    class _Serializer(SerializerMixin):
        function_id = attr.ib(type=str)

    def __init__(self, object_id=None):
        super().__init__(object_id=object_id)


class AuthBKData(BaseBKDataObject):
    auth_object_class = "bkdata"
    input_val_key = "*"


class AuthDataAdmin(BaseBKDataObject):
    auth_object_class = "dataadmin"


class AuthDataToken(BaseBKDataObject):
    auth_object_class = "data_token"
    input_val_key = "data_token_id"
    outer_model = DataTokenInfo

    @attr.s
    class _Serializer(SerializerMixin):
        data_token_id = attr.ib(type=int)

    def __init__(self, object_id=None):
        super().__init__(object_id=object_id)


class AuthResourceGroupInfo(BaseBKDataObject):
    auth_object_class = "resource_group"
    input_val_key = "resource_group_id"
    outer_model = ResourceGroupInfo

    @attr.s
    class _Serializer(SerializerMixin):
        resource_group_id = attr.ib(type=str)


class AuthAlgoModel(BaseBKDataObject):
    auth_object_class = "model"
    input_val_key = "model_id"
    outer_model = AlgoModel

    metadata_dgraph_table = "ModelInfo"
    metadata_type = "model_info"

    @attr.s
    class _Serializer(SerializerMixin):
        model_id = attr.ib(type=str, metadata={"dgraph_field": "ModelInfo.model_id"})
        project_id = attr.ib(type=int, metadata={"dgraph_field": "ModelInfo.project_id"})
        sensitivity = attr.ib(type=str, metadata={"dgraph_field": "ModelInfo.sensitivity", "is_dimension": True})

    def search_object(self):
        try:
            return AlgoModel.objects.get(model_id=self.object_id)
        except (AlgoModel.DoesNotExist, ValueError):
            pass

        return None


class AuthAlgoSampleSet(BaseBKDataObject):
    auth_object_class = "sample_set"
    input_val_key = "sample_set_id"
    outer_model = AlgoSampleSet

    metadata_dgraph_table = "SampleSet"
    metadata_type = "sample_set"

    @attr.s
    class _Serializer(SerializerMixin):
        sample_set_id = attr.ib(type=int, metadata={"dgraph_field": "SampleSet.id"})
        project_id = attr.ib(type=int, metadata={"dgraph_field": "SampleSet.project_id"})
        sensitivity = attr.ib(type=str, metadata={"dgraph_field": "SampleSet.sensitivity", "is_dimension": True})

    def search_object(self):
        try:
            return AlgoSampleSet.objects.get(sample_set_id=self.object_id)
        except (AlgoSampleSet.DoesNotExist, ValueError):
            pass

        return None


class AuthAlgorithm(BaseBKDataObject):
    auth_object_class = "algorithm"
    input_val_key = "algorithm_name"
    outer_model = Algorithm

    metadata_dgraph_table = "Algorithm"
    metadata_type = "algorithm"

    @attr.s
    class _Serializer(SerializerMixin):
        algorithm_name = attr.ib(type=str, metadata={"dgraph_field": "Algorithm.algorithm_name"})
        project_id = attr.ib(type=int, metadata={"dgraph_field": "Algorithm.project_id"})
        sensitivity = attr.ib(type=str, metadata={"dgraph_field": "Algorithm.sensitivity", "is_dimension": True})

    def search_object(self):
        try:
            return Algorithm.objects.get(algorithm_name=self.object_id)
        except (AlgoSampleSet.DoesNotExist, ValueError):
            pass
        return None


class AuthDataModel(BaseBKDataObject):
    auth_object_class = "datamodel"
    input_val_key = "model_id"
    outer_model = DataModel
    parent_models = [ProjectInfo]

    @attr.s
    class _Serializer(SerializerMixin):
        project_id = attr.ib(type=int)
        model_id = attr.ib(type=int)

    def __init__(self, object_id=None):
        super().__init__(object_id=object_id)

    def search_object(self):
        try:
            return DataModel.objects.get(model_id=self.object_id)
        except (DataModel.DoesNotExist, ValueError):
            pass

        return None


class ObjectFactory:
    BKDATA_OBJECT_CLASSES = [
        AuthBusiness,
        AuthProject,
        AuthFlow,
        AuthResultTable,
        AuthRawData,
        AuthDashboard,
        AuthFunction,
        AuthBKData,
        AuthDataAdmin,
        AuthDataToken,
        AuthResourceGroupInfo,
        AuthAlgoModel,
        AuthAlgoSampleSet,
        AuthAlgorithm,
        AuthDataModel,
    ]

    # 保留映射变量，便于直接从 object_class 找到对象定义
    BKDATA_OBJECTS = {_object.auth_object_class: _object for _object in BKDATA_OBJECT_CLASSES}

    @classmethod
    def wrap_display_all(cls, data, input_val_key=None, output_val_key=None, display_key_maps=None, key_path=None):
        """
        不确定数据对象的情况下使用，遍历所有对象并补充显示名称
        @return:
        """
        for _object in cls.BKDATA_OBJECT_CLASSES:
            if _object.outer_model is None or _object.input_val_key is None:
                continue

            # 太低效了，需要优化，没有选择性，所有对象都要全量映射
            data = _object.wrap_display(
                data,
                input_val_key=input_val_key,
                output_val_key=output_val_key,
                display_key_maps=display_key_maps,
                key_path=key_path,
            )
        return data

    @classmethod
    def init_object_by_metadata_type(cls, metadata_type):
        for _obj in cls.BKDATA_OBJECT_CLASSES:
            if _obj.metadata_type == metadata_type:
                return _obj

        raise NotObjectClassSameWithMeta()

    @classmethod
    def init_object_by_class(cls, object_class):
        for _obj in cls.BKDATA_OBJECT_CLASSES:
            if _obj.auth_object_class == object_class:
                return _obj

        raise NotImplementedError

    @classmethod
    def init_instance_by_class(cls, object_class, object_id):
        return cls.init_object_by_class(object_class)(object_id)

    @classmethod
    def init_object(cls, action_id):
        """
        根据action_id初始化权限对象，不建议批量调用
        @param action_id:
        @return:
        """
        return cls.init_object_by_class(ActionConfig.objects.get(pk=action_id).object_class_id)

    @classmethod
    def init_instance(cls, action_id, object_id):
        return cls.init_object(action_id)(object_id)


oFactory = ObjectFactory()
