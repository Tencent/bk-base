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
from collections import namedtuple

from common.base_utils import custom_params_valid, model_to_dict
from common.django_utils import CustomJSONEncoder
from common.local import get_request_username
from common.transaction import meta_sync_register
from django.db import models
from django.utils.translation import ugettext_lazy as _

from meta import exceptions as meta_errors
from meta.basic.common import RPCMixIn
from meta.configs.models import ProcessingTypeConfig
from meta.public.models import AuditMixin, DelMixin
from meta.public.models.result_table import ResultTable
from meta.public.models.result_table_action import ResultTableActions
from meta.public.serializers.result_table import (
    ResultTableSerializer,
    ResultTableUpdateSerializer,
)
from meta.utils.basicapi import parseresult
from meta.utils.common import ObjectProxy


class DataProcessingManager(RPCMixIn, models.Manager):
    rt_actions = ResultTableActions()

    @staticmethod
    def check_processing_type(processing_type):
        if ProcessingTypeConfig.objects.filter(processing_type_name=processing_type).count() == 0:
            raise meta_errors.DataProcessingTypeNotExistError(message_kv={"processing_type": processing_type})

    def check_processing_stream_legality(self, request, params):
        """
        检查dataprocessing上下游合法性
        :param request: 请求对象
        :param params: 传入参数对象
        :return: boolean True/False
        """
        processing_type = params.get("processing_type", None)
        inputs = params.get("inputs", [])
        result_tables = params.get("result_tables", [])
        if processing_type not in ("queryset", "snapshot"):
            return True
        biz_sets = dict(input=set(), output=set())
        data_id_sets = dict(result_table=set(), raw_data=set())
        if result_tables:
            for result_table in result_tables:
                biz_sets["output"].add(int(result_table["bk_biz_id"]))
        if inputs:
            for item in inputs:
                data_id_sets[item["data_set_type"]].add(str(item["data_set_id"]))
            ret_biz_list_info = []
            if data_id_sets["result_table"]:
                retrieve_args = [
                    {
                        "?:typed": "ResultTable",
                        "?:start_filter": 'result_table_id in ["%s"]' % '","'.join(data_id_sets["result_table"]),
                        "bk_biz_id": True,
                    }
                ]
                ret = self.entity_query_via_erp(retrieve_args, backend_type="dgraph", version=2)
                if "ResultTable" in ret.result:
                    ret_biz_list_info.extend(ret.result["ResultTable"])
            if data_id_sets["raw_data"]:
                retrieve_args = [
                    {
                        "?:typed": "AccessRawData",
                        "?:start_filter": "id in [%s]" % ",".join(data_id_sets["raw_data"]),
                        "bk_biz_id": True,
                    }
                ]
                ret = self.entity_query_via_erp(retrieve_args, backend_type="dgraph", version=2)
                if "AccessRawData" in ret.result:
                    ret_biz_list_info.extend(ret.result["AccessRawData"])
            biz_list = [int(biz_item["bk_biz_id"]) for biz_item in ret_biz_list_info if "bk_biz_id" in biz_item]
            biz_sets["input"].update(biz_list)
        if len(biz_sets["input"]) > 1:
            return False
        if biz_sets["output"] and (biz_sets["input"] != biz_sets["output"]):
            raise meta_errors.DataProcessingRelationInputIllegalityError(
                message="stream is illegal, {msg}", message_kv={"msg": "input biz is inconsistent with output biz"}
            )
        return True

    @staticmethod
    def generate_relation_key(relation, data_directing=None):
        if isinstance(relation, dict):
            relation = namedtuple("Relation", list(relation.keys()))(*list(relation.values()))
        data_directing = data_directing or relation.data_directing
        if relation.storage_type == "channel":
            storage_id = relation.channel_cluster_config_id
        elif relation.storage_type == "storage":
            storage_id = relation.storage_cluster_config_id
        else:
            storage_id = None
        return "{}_{}_{}_{}_{}".format(
            data_directing, relation.data_set_type, relation.data_set_id, relation.storage_type, storage_id
        )

    def create_data_processing(self, request, params):
        self.check_processing_type(params["processing_type"])

        inputs = params.pop("inputs", [])
        outputs = params.pop("outputs", [])
        tags = parseresult.get_tag_params(params)

        # 如果包含result_table，会先创建结果表
        result_table_configs = params.pop("result_tables", [])
        for result_table_config in result_table_configs:
            result_table_config["project_id"] = params["project_id"]
            result_table_config["processing_type"] = params["processing_type"]
            self.operate_result_table(
                request,
                "create",
                result_table_config["result_table_id"],
                {result_table_config["result_table_id"]: result_table_config},
            )

        params["created_by"] = params.pop("bk_username", get_request_username())
        self.create(**params)

        parseresult.create_tag_to_data_processing(tags, params["processing_id"])

        for input_info in inputs:
            input_info["data_directing"] = "input"
            input_info["processing_id"] = params["processing_id"]
            input_tags = parseresult.get_tag_params(input_info)
            DataProcessingRelation.objects.create(**input_info)
            # 在input relation上打tag
            if input_tags:
                input_relation_ids = [
                    ret_item["id"]
                    for ret_item in DataProcessingRelation.objects.filter(
                        processing_id=input_info["processing_id"]
                    ).values("id")
                ]
                if input_relation_ids:
                    input_relation_id = input_relation_ids.pop()
                else:
                    raise meta_errors.DataProcessingRelationNotExistError(_("输入数据处理关联创建失败"))
                parseresult.create_tag_to_dpr(input_tags, input_relation_id)

        for output_info in outputs:
            output_info["data_directing"] = "output"
            output_info["processing_id"] = params["processing_id"]
            output_tags = parseresult.get_tag_params(output_info)
            DataProcessingRelation.objects.create(**output_info)
            # output relation上打tag
            if output_tags:
                output_relation_ids = [
                    ret_item["id"]
                    for ret_item in DataProcessingRelation.objects.filter(
                        processing_id=output_info["processing_id"]
                    ).values("id")
                ]
                if output_relation_ids:
                    output_relation_id = output_relation_ids.pop()
                else:
                    raise meta_errors.DataProcessingRelationNotExistError(_("输出数据处理关联创建失败"))
                parseresult.create_tag_to_dpr(output_tags, output_relation_id)

    def update_data_processing(self, request, data_processing, params):
        # 检测数据处理类型
        # 不允许修改数据处理类型
        # if 'processing_type' in params:
        #     self.check_processing_type(params['processing_type'])
        inputs = params.pop("inputs", [])
        outputs = params.pop("outputs", [])

        # 更新data_processing信息
        data_processing.updated_by = params.pop("bk_username", get_request_username())
        for key, value in list(params.items()):
            setattr(data_processing, key, value)
        data_processing.save()

        old_relations = {}
        old_result_tables = set()
        new_relation_params = []

        def create_new_relations():
            for param in new_relation_params:
                relation_tags = parseresult.get_tag_params(param)
                DataProcessingRelation.objects.create(**param)
                # 如果关联上有tag，创建tag
                if relation_tags:
                    relation_ids = [
                        ret_item["id"]
                        for ret_item in DataProcessingRelation.objects.filter(
                            processing_id=param["processing_id"]
                        ).values("id")
                    ]
                    if relation_ids:
                        relation_id = relation_ids.pop()
                    else:
                        raise meta_errors.DataProcessingRelationNotExistError(_("更新数据处理关联失败"))
                    parseresult.create_tag_to_dpr(relation_tags, relation_id)

        # 找出所有旧的关系
        for item in DataProcessingRelation.objects.filter(processing_id=data_processing.processing_id):
            key = self.generate_relation_key(item)
            old_relations[key] = item
            if item.data_set_type == "result_table" and item.data_directing == "output":
                old_result_tables.add(item.data_set_id)

        # 更新result_tables的信息
        rt_need_to_update = False
        result_table_configs = {}
        if "result_tables" in params:
            result_tables = params.pop("result_tables", [])
            result_table_configs = {item["result_table_id"]: item for item in result_tables}
            rt_need_to_update = True

        # 根据参数和旧关系决定是否需要新建新的关联关系
        for input_info in inputs:
            relation_key = self.generate_relation_key(input_info, "input")
            if relation_key in old_relations:
                old_relations.pop(relation_key)
                # 暂时不支持对关联关系的属性进行更新
                # for key, value in input_info.items():
                #     setattr(relation, key, value)
                #     relation.save()
            else:
                input_info["data_directing"] = "input"
                input_info["processing_id"] = data_processing.processing_id
                new_relation_params.append(input_info)

        for output_info in outputs:
            # 先建RT.
            if rt_need_to_update is True and output_info["data_set_type"] == "result_table":
                if output_info["data_set_id"] in old_result_tables:
                    old_result_tables.remove(output_info["data_set_id"])
                    self.operate_result_table(request, "update", output_info["data_set_id"], result_table_configs)
                else:
                    cnt = ResultTable.objects.filter(result_table_id=output_info["data_set_id"]).count()
                    if cnt:
                        self.operate_result_table(request, "update", output_info["data_set_id"], result_table_configs)
                        continue
                    self.operate_result_table(request, "create", output_info["data_set_id"], result_table_configs)

            # 再处理relation。
            relation_key = self.generate_relation_key(output_info, "output")
            if relation_key in old_relations:
                old_relations.pop(relation_key)
                # 暂时不支持对关联关系的属性进行更新；input,output关系必须删除后重建，否则会影响元数据同步。
                # for key, value in output_info.items():
                #     setattr(relation, key, value)
                #     relation.save()
            else:
                output_info["data_directing"] = "output"
                output_info["processing_id"] = data_processing.processing_id
                new_relation_params.append(output_info)

        # 创建新的关系并删除旧的关系
        for relation in list(old_relations.values()):
            input_relation = inputs if relation.data_directing == "input" else outputs
            if input_relation:
                relation.delete()
        create_new_relations()

        # 对于没有更新关联关系的rt，且参数中包含的rt，更新rt的基本信息和字段信息
        for result_table_id in old_result_tables:
            if result_table_id in result_table_configs:
                self.operate_result_table(request, "update", result_table_id, result_table_configs)

    def operate_result_table(self, request, operate, result_table_id, result_table_configs=None):
        result_table_config = {}
        result_table = None
        if operate in ["update", "create"]:
            result_table_configs = result_table_configs or {}
            if result_table_id not in result_table_configs:
                raise meta_errors.DataProcessingOutputConflictError(
                    _("参数result_tables中缺少{}的配置").format(result_table_id)
                )
            result_table_config = result_table_configs[result_table_id]
        if operate in ["update", "delete"]:
            try:
                result_table = ResultTable.objects.get(result_table_id=result_table_id)
            except ResultTable.DoesNotExist:
                raise meta_errors.ResultTableNotExistError(_("旧关系中结果表({})不存在").format(result_table_id))
        if operate == "create":
            result_table_config["bk_username"] = get_request_username()
            validated_data = custom_params_valid(ResultTableSerializer, result_table_config)
            validated_data.pop("bk_username")
            request_proxy = ObjectProxy(request)
            request_proxy.POST = validated_data
            request_proxy.from_view = True
            return self.rt_actions.run_create(request=request_proxy, params=validated_data)
        if operate == "update":
            result_table_config["bk_username"] = get_request_username()
            validated_data = custom_params_valid(ResultTableUpdateSerializer, result_table_config)
            validated_data.pop("bk_username")
            request_proxy = ObjectProxy(request)
            request_proxy.POST = validated_data
            request_proxy.from_view = True
            return self.rt_actions.run_update(request=request_proxy, result_table=result_table, params=validated_data)
        if operate == "delete":
            request_proxy = ObjectProxy(request)
            request_proxy.POST = {}
            return self.rt_actions.run_destroy(request, result_table)

    def delete_data_processing(self, request, data_processing, with_data=False):
        inputs = DataProcessingRelation.objects.filter(
            data_directing="input", processing_id=data_processing.processing_id
        )
        outputs = DataProcessingRelation.objects.filter(
            data_directing="output", processing_id=data_processing.processing_id
        )

        # 把当前数据处理写入删除表
        data_processing_content = model_to_dict(data_processing)
        data_processing_content["inputs"] = inputs.values("data_set_type", "data_set_id")
        data_processing_content["outputs"] = outputs.values("data_set_type", "data_set_id")
        DataProcessingDel.objects.create(
            processing_id=data_processing.processing_id,
            processing_type=data_processing.processing_type,
            processing_content=json.dumps(data_processing_content, cls=CustomJSONEncoder),
            status="deleted",
            deleted_by=get_request_username(),
        )

        result_table_ids = []
        if with_data:
            # 删除下游关联表
            for relation in outputs.filter(data_set_type="result_table"):
                result_table_ids.append(relation.data_set_id)

        # 删除当前数据处理
        inputs.delete()
        outputs.delete()

        data_processing.delete()

        for result_table_id in result_table_ids:
            self.operate_result_table(request=request, operate="delete", result_table_id=result_table_id)


class DataProcessing(AuditMixin):
    processing_id = models.CharField(
        _("数据处理ID"), primary_key=True, max_length=255, error_messages={"unique": _("数据处理({processing_id})已存在")}
    )
    project_id = models.IntegerField(_("项目ID"))
    processing_alias = models.CharField(_("数据处理名称"), max_length=255)
    processing_type = models.CharField(_("数据处理类型"), max_length=32)
    generate_type = models.CharField(_("生成类型"), max_length=32, default="user")
    description = models.TextField(_("备注信息"))
    platform = models.CharField(_("所属平台"), max_length=32, default="bkdata")

    objects = DataProcessingManager()

    class Meta:
        db_table = "data_processing"
        app_label = "public"
        managed = False


class DataProcessingDel(DelMixin):
    id = models.AutoField(primary_key=True)
    processing_id = models.CharField(primary_key=True, max_length=255)
    processing_content = models.TextField(_("数据处理逻辑内容，使用json格式"))
    processing_type = models.CharField(_("数据处理类型"), max_length=32)

    class Meta:
        db_table = "data_processing_del"
        app_label = "public"
        managed = False


class DataProcessingRelation(models.Model):
    id = models.AutoField(primary_key=True)
    data_directing = models.CharField(
        _("数据方向"),
        max_length=128,
        choices=(
            ("input", _("输入")),
            ("output", _("输出")),
        ),
        null=True,
        blank=True,
    )
    data_set_type = models.CharField(
        _("数据类型"),
        max_length=128,
        choices=(
            ("raw_data", _("源数据")),
            ("result_table", _("结果表")),
        ),
        null=True,
        blank=True,
    )
    data_set_id = models.CharField(_("数据ID"), max_length=255, null=True, blank=True)
    storage_cluster_config_id = models.IntegerField(_("存储信息ID"), null=True, blank=True)
    channel_cluster_config_id = models.IntegerField(_("channel ID"), null=True, blank=True)
    storage_type = models.CharField(
        _("数据处理存储类型"),
        null=True,
        blank=True,
        max_length=32,
        choices=(
            ("channel", _("管道")),
            ("storage", _("存储")),
        ),
    )
    processing_id = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        db_table = "data_processing_relation"
        app_label = "public"
        managed = False


meta_sync_register(DataProcessing)
meta_sync_register(DataProcessingRelation)
