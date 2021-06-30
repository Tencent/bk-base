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

from auth.exceptions import OuterModelAttrErr
from common.base_utils import model_to_dict
from common.business import Business
from common.fields import TransCharField
from common.log import logger
from django.core.cache import cache
from django.db import models
from django.utils.translation import ugettext_lazy as _


class OuterModel(models.Model):
    """
    外部模型，仅用于查询使用
    """

    # 子类需修改display_name_key 作为输出名
    display_name_key = "display_name"
    description_key = "description"

    # 允许进行过滤的键
    allowed_filter_keys = []

    # 字典表缓存时间
    DISPLAY_DICT_CACHE_TIME = 300

    @classmethod
    def wrap_display(cls, data, input_val_key, display_key_maps=None, output_val_key=None, key_path=None, filters=None):
        """
        数据列表，统一加上显示名称
        尽量避免在循环中调用此方法，而是将data作为列表传入，被翻译的id可位于列表对象中的自定义层级
        @param {List} data 需补充显示名的数据列表
        @param {String} input_val_key 输入的key值
        @param {Dict} display_key_maps 输出显示名的映射
        @param {List} key_path val_key在data中所在的字典路径
        @param {Dict} filters 过滤条件
        @paramExample 参数样例
        {
            "data": [
                {
                    "xxx": {
                        "yyy": {
                            "bk_biz_id": 3
                        }
                    }
                }
            ],
            "input_val_key": "bk_biz_id",
            "display_key_maps": {
                "display_name_key": "bk_biz_name",
                "description_key": "description",
            },
            "key_path": ["xxx", "yyy"],
            "filters": {
                "bk_biz_id": 2
            }
        }
        @successExample 成功返回
        [
            {
                "xxx": {
                    "yyy": {
                        "bk_biz_id": 3
                        "bk_biz_name": "蓝鲸",
                        "description": "蓝鲸的描述"
                    }
                }
            }
        ]
        """
        if key_path is None:
            key_path = []
        if display_key_maps is None:
            display_key_maps = {}

        try:
            display_dict = cls.get_display_dict(display_key_maps=display_key_maps, filters=filters)
        except Exception as e:
            logger.exception(f"[DB WRAP DISPLAY] Failed to read DB Table, cls={cls}, error={e}")

            # 当DB或Table不存在时，会触发此异常
            display_dict = {}

        cls._recursion_wrap(data, input_val_key, output_val_key, key_path, display_dict)

        return data

    @classmethod
    def _recursion_wrap(cls, _data, input_val_key, output_val_key, key_path, display_dict):
        # 递归写入显示名

        if isinstance(_data, dict):

            _temp_data = _data
            sub_path_index = 0
            for sub_key_path in key_path:
                if isinstance(_temp_data, dict):
                    _temp_data = _temp_data[sub_key_path]
                    sub_path_index += 1
                    cls._recursion_wrap(
                        _temp_data, input_val_key, output_val_key, key_path[sub_path_index:], display_dict
                    )

                if isinstance(_temp_data, list):
                    for _next in _temp_data:
                        cls._recursion_wrap(
                            _next, input_val_key, output_val_key, key_path[sub_path_index:], display_dict
                        )

            if len(key_path) == 0:
                try:
                    value_id = str(_temp_data.get(input_val_key))
                except Exception:
                    value_id = _temp_data.get(input_val_key)
                _temp_data.update(display_dict.get(value_id, {}))
                if output_val_key and input_val_key in _temp_data:
                    _temp_data[output_val_key] = _temp_data.pop(input_val_key)
                return

        elif isinstance(_data, list):
            for _temp_data in _data:
                cls._recursion_wrap(_temp_data, input_val_key, output_val_key, key_path, display_dict)
        else:
            raise TypeError

    @classmethod
    def get_display_dict(cls, display_key_maps, filters=None):
        """description_key"""
        display_name_key = display_key_maps.get("display_name_key") or cls.display_name_key
        description_key = display_key_maps.get("description_key") or cls.description_key

        if filters is None:
            filters = {}

        cache_key = "display_dict:{class_name}:{display_name_key}:{description_key}:{filters}".format(
            class_name=cls.__name__,
            display_name_key=display_name_key,
            description_key=description_key,
            filters=json.dumps(filters),
        )
        _cache = cache.get(cache_key)
        if _cache:
            return _cache
        else:
            data = cls._get_display_dict(display_name_key, description_key, filters)
            cache.set(cache_key, data, cls.DISPLAY_DICT_CACHE_TIME)
            return data

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        """description_key"""
        raise NotImplementedError

    @classmethod
    def list(cls, filters):
        """
        Model 全量列表
        @param {Dict} filter 以属性对作为过滤条件，可直接作为 ORM 过滤条件
        """
        return [model_to_dict(_object) for _object in cls.objects.filter(**filters)]

    class Meta:
        abstract = True


class BusinessInfo(OuterModel):
    display_name_key = "bk_biz_name"

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_bizs = Business.list()
        return {
            str(biz["bk_biz_id"]): {
                display_name_key: "[{id}]{name}".format(id=str(biz["bk_biz_id"]), name=biz.get("bk_biz_name")),
                description_key: biz.get("description"),
            }
            for biz in all_bizs
        }

    @classmethod
    def list(cls, filters, **kwargs):
        return Business.list()

    class Meta:
        db_table = "biz_info"
        app_label = "basic"
        managed = False


class FlowInfo(OuterModel):
    display_name_key = "flow_name"
    allowed_filter_keys = ["project_id"]

    flow_id = models.AutoField(primary_key=True)
    flow_name = models.CharField(_("作业名称"), max_length=255)
    project_id = models.IntegerField(_("项目ID"))
    description = models.TextField(_("备注信息"), blank=True, null=True)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_flows = cls.objects.filter(**filters)
        return {
            str(flow.flow_id): {
                display_name_key: "[{id}]{name}".format(id=str(flow.flow_id), name=flow.flow_name),
                description_key: flow.description,
            }
            for flow in all_flows
        }

    class Meta:
        db_table = "dataflow_info"
        app_label = "flow"
        managed = False


class PROCESSING_TYPE:
    CLEAN = "clean"


class DATA_SCENARIO:
    TDM = "tdm"


class ResultTable(OuterModel):
    display_name_key = "result_table_name"
    allowed_filter_keys = ["project_id", "bk_biz_id"]

    result_table_id = models.CharField(_("结果表标识"), max_length=255, primary_key=True)
    bk_biz_id = models.IntegerField(_("业务ID"))
    project_id = models.IntegerField(_("项目ID"))
    sensitivity = models.CharField(max_length=32, blank=True, null=True)
    result_table_name = models.CharField(_("结果表名"), max_length=255)
    result_table_name_alias = models.CharField(_("别名"), max_length=255)
    result_table_type = models.CharField(_("结果表类型"), max_length=32, blank=True, null=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)
    processing_type = models.CharField(_("计算类型"), max_length=32)
    generate_type = models.CharField(_("结果表生成类型"), max_length=32, default="user")

    @classmethod
    def list(cls, filters):
        # 读取列表接口，默认仅返回用户表
        filters["generate_type"] = "user"
        return [model_to_dict(_object) for _object in cls.objects.filter(**filters)]

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_result_tables = cls.objects.filter(**filters).values(
            "result_table_id", "result_table_name", "result_table_name_alias"
        )
        return {
            result_table["result_table_id"]: {
                display_name_key: result_table["result_table_id"],
                description_key: result_table["result_table_name_alias"],
            }
            for result_table in all_result_tables
        }

    def get_parent_raw_data_id(self):
        """
        获取清洗数据类型
        """
        if self.processing_type != PROCESSING_TYPE.CLEAN:
            raise OuterModelAttrErr(_("目前仅支持清洗表查询关联的原始数据"))

        # 清洗表必然存在父级原始数据
        relations = DataProcessingRelation.objects.filter(
            processing_id=self.result_table_id, data_directing="input"
        ).only("data_set_id")
        if not relations.exists():
            raise OuterModelAttrErr(_(f"清洗表（{self.result_table_id}）无法追溯到原始数据"))

        return relations[0].data_set_id

    class Meta:
        db_table = "result_table"
        app_label = "basic"
        managed = False


class DataProcessingRelation(OuterModel):
    data_directing = models.CharField(_("血缘方向"))
    data_set_type = models.CharField(_("数据源类型"))
    data_set_id = models.CharField(_("数据ID"))
    processing_id = models.CharField(_("处理ID"))

    class Meta:
        db_table = "data_processing_relation"
        app_label = "basic"
        managed = False


class AccessRawData(OuterModel):
    display_name_key = "raw_data_name"
    allowed_filter_keys = ["bk_biz_id"]

    raw_data_id = models.IntegerField(db_column="id")
    bk_biz_id = models.IntegerField()
    raw_data_name = models.CharField(max_length=128)
    raw_data_alias = models.CharField(max_length=128)
    sensitivity = models.CharField(max_length=32, blank=True, null=True)
    data_source = models.CharField(max_length=32)
    data_encoding = models.CharField(max_length=32, blank=True, null=True)
    data_category = models.CharField(max_length=32, blank=True, null=True)
    data_scenario = models.CharField(max_length=128)
    maintainer = models.CharField(max_length=255, blank=True, null=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    @classmethod
    def raw_data_id_list(cls):
        return AccessRawData.objects.values_list("id", flat=True)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_raw_datas = cls.objects.filter(**filters).values("id", "raw_data_name", "raw_data_alias")
        return {
            str(raw_data["id"]): {
                display_name_key: "[{id}]{raw_data_name}".format(
                    id=str(raw_data["id"]),
                    raw_data_name=raw_data["raw_data_name"],
                ),
                description_key: raw_data["raw_data_alias"],
            }
            for raw_data in all_raw_datas
        }

    @classmethod
    def list(cls, filters):
        """
        原始数据全量列表，由于主键是id，而不是raw_data_id，这里特殊处理补充raw_data_id
        @return:
        """
        data = [model_to_dict(_object) for _object in cls.objects.filter(**filters)]
        for _d in data:
            _d["raw_data_id"] = _d["id"]
        return data

    @classmethod
    def list_etl_result_table_ids_batch(cls, raw_data_ids):
        """
        批量查询原始数据关联的 ETL
        """
        relations = DataProcessingRelation.objects.filter(data_set_id__in=raw_data_ids, data_directing="input").only(
            "processing_id"
        )

        return [r.processing_id for r in relations]

    @property
    def maintainer_list(self):
        return self.maintainer.split(",")

    class Meta:
        db_table = "access_raw_data"
        app_label = "basic"
        managed = False


class ProjectInfo(OuterModel):
    display_name_key = "project_name"
    allowed_filter_keys = ["project_id"]

    project_id = models.AutoField(primary_key=True)
    project_name = TransCharField(max_length=255)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_projects = cls.objects.filter(**filters).values("project_id", "project_name", "description")
        return {
            str(project["project_id"]): {
                display_name_key: "[{id}]{name}".format(id=str(project["project_id"]), name=project["project_name"]),
                description_key: project["description"],
            }
            for project in all_projects
        }

    class Meta:
        db_table = "project_info"
        app_label = "basic"
        managed = False


class ClusterGroupConfig(OuterModel):
    """
    集群组配置
    """

    display_name_key = "cluster_group_name"

    cluster_group_id = models.CharField(_("集群组ID"), primary_key=True, max_length=255)
    cluster_group_name = models.CharField(_("集群组名称"), max_length=255)
    cluster_group_alias = models.CharField(_("集群中文名"), max_length=255, null=True, blank=True)
    scope = models.CharField(_("集群可访问性"), max_length=255, choices=(("private", _("私有")), ("public", _("公共"))))
    created_by = models.CharField(_("创建人"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("修改人"), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_("修改时间"), auto_now=True, blank=True, null=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_cluster_groups = cls.objects.filter(**filters)
        return {
            cluster_group.cluster_group_id: {
                display_name_key: "[{cluster_group_name}]{cluster_group_alias}".format(
                    cluster_group_name=cluster_group.cluster_group_name,
                    cluster_group_alias=cluster_group.cluster_group_alias,
                ),
                description_key: cluster_group.description,
            }
            for cluster_group in all_cluster_groups
        }

    class Meta:
        managed = False
        db_table = "cluster_group_config"
        app_label = "basic"


class DashboardInfo(OuterModel):
    display_name_key = "dashboard_name"
    allowed_filter_keys = ["project_id"]

    dashboard_id = models.AutoField(primary_key=True, db_column="id")
    dashboard_name = models.CharField(_("图表名称"), max_length=500, db_column="dashboard_title")
    project_id = models.IntegerField(_("项目ID"))
    description = models.TextField(_("备注信息"), blank=True, null=True)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_dashboards = cls.objects.filter(**filters).values("dashboard_id", "dashboard_name", "description")
        return {
            str(dashboard["dashboard_id"]): {
                display_name_key: "[{id}]{name}".format(
                    id=str(dashboard["dashboard_id"]), name=dashboard["dashboard_name"]
                ),
                description_key: dashboard["description"],
            }
            for dashboard in all_dashboards
        }

    class Meta:
        db_table = "dashboards"
        app_label = "superset"
        managed = False


class UDFManager(models.Manager):
    """
    目前自定义函数对象没有唯一的主体表，从函数版本记录表中获取，请注意 function_id 需要去重
    """

    def get_queryset(self):
        return super().get_queryset().distinct()


class FunctionInfo(OuterModel):
    """
    目前自定义函数对象没有唯一的主体表，从函数版本记录表中获取，请注意 function_id 需要去重
    """

    display_name_key = "function_id"
    description_key = "function_id"

    function_id = models.CharField(primary_key=True, db_column="func_name")

    objects = UDFManager()

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_functions = cls.objects.filter(**filters).values("function_id")

        return {
            str(o_function["function_id"]): {
                display_name_key: "{id}".format(id=str(o_function["function_id"])),
                description_key: o_function["function_id"],
            }
            for o_function in all_functions
        }

    class Meta:
        db_table = "bksql_function_dev_config"
        app_label = "flow"
        managed = False


class DataTokenInfo(OuterModel):
    """
    目前自定义函数对象没有唯一的主体表，从函数版本记录表中获取，请注意 function_id 需要去重
    """

    display_name_key = "data_token_name"

    data_token_id = models.IntegerField(primary_key=True, db_column="id")
    data_token_bk_app_code = models.CharField(max_length=64)
    description = models.TextField(blank=True, null=True)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_objects = cls.objects.filter(**filters).values("data_token_id", "description", "data_token_bk_app_code")

        return {
            str(_obj["data_token_id"]): {
                display_name_key: _("[{id}] {app_code} 授权码").format(
                    id=_obj["data_token_id"], app_code=_obj["data_token_bk_app_code"]
                ),
                description_key: _obj["description"],
            }
            for _obj in all_objects
        }

    class Meta:
        db_table = "auth_data_token"
        app_label = "basic"
        managed = False


class ResourceGroupInfo(OuterModel):
    """
    目前自定义函数对象没有唯一的主体表，从函数版本记录表中获取，请注意 function_id 需要去重
    """

    display_name_key = "group_name"

    resource_group_id = models.CharField(primary_key=True, max_length=45)
    group_name = models.CharField(max_length=255)
    group_type = models.CharField(max_length=45)
    bk_biz_id = models.IntegerField()
    status = models.CharField(max_length=45)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_objects = cls.objects.filter(**filters).values("resource_group_id", "group_name", "description")

        return {
            _obj["resource_group_id"]: {
                display_name_key: "[{}] {}".format(_obj["resource_group_id"], _obj["group_name"]),
                description_key: _obj["description"],
            }
            for _obj in all_objects
        }

    class Meta:
        db_table = "resource_group_info"
        app_label = "basic"
        managed = False


class ResourceGeogAreaClusterGroup(OuterModel):
    """
    资源组
    """

    resource_group_id = models.CharField(_("资源组英文标识"), max_length=256, primary_key=True)
    geog_area_code = models.CharField(_("区域"), max_length=256)
    cluster_group = models.CharField(_("资源组ID"), max_length=256)

    class Meta:
        managed = False
        app_label = "basic"
        db_table = "resource_geog_area_cluster_group"


class AlgoModel(OuterModel):
    display_name_key = "model_name"

    model_id = models.CharField(_("模型ID"), max_length=128, primary_key=True)
    model_name = models.CharField(_("模型名称"), max_length=64)
    model_alias = models.CharField(_("模型别名"), max_length=64)
    description = models.TextField(_("描述"))
    project_id = models.IntegerField(_("项目ID"))
    sensitivity = models.CharField(_("敏感度"), max_length=32, default="private")
    active = models.IntegerField(_("是否激活"), default=1)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_objects = list(cls.objects.filter(**filters).values())

        return {
            str(_obj["model_id"]): {
                display_name_key: "[{id}] {name}".format(id=_obj["model_id"], name=_obj["model_name"]),
                description_key: _obj["description"],
            }
            for _obj in all_objects
        }

    class Meta:
        managed = False
        app_label = "modeling"
        db_table = "model_info"


class AlgoSampleSet(OuterModel):
    display_name_key = "sample_set_name"

    sample_set_id = models.IntegerField(_("样本集ID"), primary_key=True, db_column="id")
    sample_set_name = models.CharField(_("样本集名称"), max_length=255)
    sample_set_alias = models.CharField(_("样本集别名"), max_length=255)
    description = models.TextField(_("描述"))
    project_id = models.IntegerField(_("项目ID"))
    sensitivity = models.CharField(_("敏感度"), max_length=32, default="private")
    active = models.IntegerField(_("是否激活"), default=1)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_objects = list(cls.objects.filter(**filters).values())

        return {
            str(_obj["sample_set_id"]): {
                display_name_key: "[{id}] {name}".format(id=_obj["sample_set_id"], name=_obj["sample_set_name"]),
                description_key: _obj["description"],
            }
            for _obj in all_objects
        }

    class Meta:
        managed = False
        app_label = "modeling"
        db_table = "sample_set"


class Algorithm(OuterModel):
    display_name_key = "algorithm_name"

    algorithm_name = models.CharField(_("算法名称"), max_length=255, primary_key=True, db_column="algorithm_name")
    algorithm_alias = models.CharField(_("算法别名"), max_length=255)
    description = models.TextField(_("描述"))
    sensitivity = models.CharField(_("敏感度"), max_length=32, default="private")
    project_id = models.IntegerField(_("项目ID"))

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_objects = list(cls.objects.filter(**filters).values())
        return {
            str(_obj["algorithm_id"]): {
                display_name_key: "[{id}] {name}".format(id=_obj["algorithm_name"], name=_obj["algorithm_name"]),
                description_key: _obj["description"],
            }
            for _obj in all_objects
        }

    class Meta:
        managed = False
        app_label = "modeling"
        db_table = "algorithm"


class DataModel(OuterModel):
    display_name_key = "model_name"
    allowed_filter_keys = ["project_id"]

    model_id = models.IntegerField(_("数据模型标识"), primary_key=True)
    model_name = models.CharField(_("数据模型名称"), max_length=255)
    model_alias = models.CharField(_("数据模型别名"), max_length=255)
    project_id = models.IntegerField(_("项目ID"))
    description = models.TextField(_("数据模型描述"), blank=True, null=True)

    @classmethod
    def _get_display_dict(cls, display_name_key, description_key, filters):
        all_data_models = cls.objects.filter(**filters).values("model_id", "model_name", "model_alias")
        return {
            data_model["model_id"]: {
                display_name_key: data_model["model_id"],
                description_key: data_model["model_alias"],
            }
            for data_model in all_data_models
        }

    class Meta:
        managed = False
        app_label = "basic"
        db_table = "dmm_model_info"
