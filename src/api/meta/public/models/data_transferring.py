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

from common.base_utils import model_to_dict
from common.django_utils import CustomJSONEncoder
from common.local import get_request_username
from common.transaction import meta_sync_register
from django.db import models
from django.utils.translation import ugettext_lazy as _

from meta import exceptions as meta_errors
from meta.configs.models import TransferringTypeConfig
from meta.public.models import AuditMixin, DelMixin
from meta.utils.basicapi import parseresult


class DataTransferringManager(models.Manager):
    @staticmethod
    def check_transferring_type(transferring_type):
        if TransferringTypeConfig.objects.filter(transferring_type_name=transferring_type).count() == 0:
            raise meta_errors.DataTransferringTypeNotExistError(message_kv={"transferring_type": transferring_type})

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

    def create_data_tranferring(self, params):
        self.check_transferring_type(params["transferring_type"])

        inputs = params.pop("inputs", [])
        outputs = params.pop("outputs", [])
        tags = parseresult.get_tag_params(params)

        params["created_by"] = params.pop("bk_username", get_request_username())
        self.create(**params)
        parseresult.create_tag_to_data_transferring(tags, params["transferring_id"])

        for input_info in inputs:
            input_info["data_directing"] = "input"
            input_info["transferring_id"] = params["transferring_id"]
            DataTransferringRelation.objects.create(**input_info)

        for output_info in outputs:
            output_info["data_directing"] = "output"
            output_info["transferring_id"] = params["transferring_id"]
            DataTransferringRelation.objects.create(**output_info)

    def update_data_transferring(self, data_transferring, params):
        # 检测数据传输类型
        # 不允许修改数据传输类型
        inputs = params.pop("inputs", None)
        outputs = params.pop("outputs", None)

        # 更新data_transferring信息
        data_transferring.updated_by = params.pop("bk_username", get_request_username())
        for key, value in list(params.items()):
            setattr(data_transferring, key, value)
        data_transferring.save()

        old_relations = {}

        if inputs is not None:
            for item in DataTransferringRelation.objects.filter(
                transferring_id=data_transferring.transferring_id, data_directing="input"
            ):
                key = self.generate_relation_key(item)
                old_relations[key] = item

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
                    input_info["transferring_id"] = data_transferring.transferring_id
                    DataTransferringRelation.objects.create(**input_info)

        if outputs is not None:
            for item in DataTransferringRelation.objects.filter(
                transferring_id=data_transferring.transferring_id, data_directing="output"
            ):
                key = self.generate_relation_key(item)
                old_relations[key] = item

            for output_info in outputs:
                relation_key = self.generate_relation_key(output_info, "output")
                if relation_key in old_relations:
                    old_relations.pop(relation_key)
                    # 暂时不支持对关联关系的属性进行更新
                    # for key, value in output_info.items():
                    #     setattr(relation, key, value)
                    #     relation.save()
                else:
                    output_info["data_directing"] = "output"
                    output_info["transferring_id"] = data_transferring.transferring_id
                    DataTransferringRelation.objects.create(**output_info)

        # 创建新的关系并删除旧的关系
        for relation in list(old_relations.values()):
            relation.delete()

    @staticmethod
    def delete_data_transferring(data_transferring):
        inputs = DataTransferringRelation.objects.filter(
            data_directing="input", transferring_id=data_transferring.transferring_id
        )
        outputs = DataTransferringRelation.objects.filter(
            data_directing="output", transferring_id=data_transferring.transferring_id
        )

        # 把当前数据传输写入删除表
        data_transferring_content = model_to_dict(data_transferring)
        data_transferring_content["inputs"] = inputs.values("data_set_type", "data_set_id")
        data_transferring_content["outputs"] = outputs.values("data_set_type", "data_set_id")
        DataTransferringDel.objects.create(
            transferring_id=data_transferring.transferring_id,
            transferring_type=data_transferring.transferring_type,
            transferring_content=json.dumps(data_transferring_content, cls=CustomJSONEncoder),
            status="deleted",
            deleted_by=get_request_username(),
        )

        # 删除当前数据传输
        inputs.delete()
        outputs.delete()

        data_transferring.delete()


class DataTransferring(AuditMixin):
    transferring_id = models.CharField(primary_key=True, max_length=255)
    project_id = models.IntegerField(_("项目ID"))
    transferring_alias = models.CharField(_("数据传输中文名"), max_length=255)
    transferring_type = models.CharField(_("数据传输类型"), max_length=32)
    generate_type = models.CharField(_("生成类型"), max_length=32, default="user")
    description = models.TextField(_("备注信息"))

    objects = DataTransferringManager()

    class Meta:
        db_table = "data_transferring"
        app_label = "public"
        managed = False


class DataTransferringDel(DelMixin):
    id = models.AutoField(primary_key=True)
    transferring_id = models.CharField(primary_key=True, max_length=255)
    transferring_content = models.TextField(_("数据传输逻辑内容，使用json格式"))
    transferring_type = models.CharField(_("数据传输类型"), max_length=32)

    class Meta:
        db_table = "data_transferring_del"
        app_label = "public"
        managed = False


class DataTransferringRelation(models.Model):
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
        max_length=32,
        choices=(
            ("channel", _("管道")),
            ("storage", _("存储")),
            ("memory", _("内存")),
        ),
    )
    transferring_id = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        db_table = "data_transferring_relation"
        app_label = "public"
        managed = False


meta_sync_register(DataTransferring)
meta_sync_register(DataTransferringRelation)
