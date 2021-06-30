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

from common.base_utils import model_to_dict
from common.django_utils import CustomJSONEncoder
from common.local import get_request_username
from common.transaction import meta_sync_register
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from django.db import IntegrityError, models
from django.utils.translation import ugettext_lazy as _
from meta import exceptions as meta_errors
from meta.configs.models import FieldTypeConfig, ResultTableTypeConfig
from meta.exceptions import ResultTableFieldTypeChangedError
from meta.public.models import AuditMixin, DelMixin
from meta.utils.basicapi import parseresult


class ResultTableManager(models.Manager):
    @staticmethod
    def create_table(params):
        bk_username = params.pop("bk_username", get_request_username())
        result_table_fields = params.pop("fields")
        tags = parseresult.get_tag_params(params)
        params["created_by"] = bk_username

        # 检查结果表类型
        if "result_table_type" in params and params["result_table_type"] is not None:
            if not params["result_table_type"].isdigit():
                queryset = ResultTableTypeConfig.objects.filter(result_table_type_code=params["result_table_type"])
            else:
                queryset = ResultTableTypeConfig.objects.filter(id=int(params["result_table_type"]))
            if len(queryset) > 0:
                result_table_type_config = queryset[0]
                params["result_table_type"] = result_table_type_config.id
            else:
                raise meta_errors.ResultTableTypeNotExistError(
                    message_kv={"result_table_type": params["result_table_type"]}
                )

        # 校验字段数目
        if isinstance(result_table_fields, list) and len(result_table_fields) > settings.FIELDS_LIMIT:
            raise meta_errors.TooManyResultTableFieldError(message_kv={"fields_limit": settings.FIELDS_LIMIT})

        result_table_id = params.get("result_table_id")
        try:
            result_table = ResultTable.objects.create(**params)
        except IntegrityError as e:
            if int(e.args[0]) == 1062:
                raise meta_errors.ResultTableHasExistedError(message_kv={"result_table_id": result_table_id})
            else:
                raise meta_errors.DatabaseError(message_kv={"err_msg": e.message})
        field_types = list(FieldTypeConfig.objects.filter(active=True).values_list("field_type", flat=True))

        for index, field in enumerate(result_table_fields):
            if field["field_type"] not in field_types:
                raise meta_errors.ResultTableFieldTypeIllegalError(message_kv={"field_type": field["field_type"]})
            field["field_index"] = field.get("field_index", index)
            field["created_by"] = bk_username
            field["result_table_id"] = params["result_table_id"]

            try:
                ResultTableField.objects.create(**field)
            except IntegrityError as e:
                if int(e.args[0]) == 1062:
                    raise meta_errors.ResultTableFieldConflictError(message_kv={"field": field["field_name"]})
                else:
                    raise meta_errors.DatabaseError(message_kv={"err_msg": e.message})

        # 兼容, system queryset，暂时不打标签
        if params.get("processing_type", None) == "queryset" and params.get("generate_type", None) == "system":
            return result_table
        parseresult.create_tag_to_result_table(
            tags, result_table.bk_biz_id, result_table.project_id, result_table.result_table_id
        )
        return result_table

    def update_table(self, result_table, params):
        if "result_table_type" in params and params["result_table_type"] is not None:
            if not params["result_table_type"].isdigit():
                queryset = ResultTableTypeConfig.objects.filter(result_table_type_code=params["result_table_type"])
            else:
                queryset = ResultTableTypeConfig.objects.filter(id=int(params["result_table_type"]))
            if len(queryset) > 0:
                result_table_type_config = queryset[0]
                params["result_table_type"] = result_table_type_config.id
            else:
                raise meta_errors.ResultTableTypeNotExistError(
                    message_kv={"result_table_type": params["result_table_type"]}
                )

        bk_username = params.pop("bk_username", get_request_username())
        new_fields = params.pop("fields", None)

        # 校验字段数目
        if isinstance(new_fields, list) and len(new_fields) > settings.FIELDS_LIMIT:
            raise meta_errors.TooManyResultTableFieldError(message_kv={"fields_limit": settings.FIELDS_LIMIT})

        result_table.updated_by = bk_username
        for key, value in list(params.items()):
            setattr(result_table, key, value)
        if result_table.created_by is None:
            result_table.created_by = bk_username
        result_table.save()
        if new_fields is not None:
            self.update_table_fields(result_table, new_fields)
        return result_table

    @staticmethod
    def update_table_fields(result_table, fields):
        bk_username = get_request_username()
        # 提取旧字段信息时，规避旧数据字段中存在多个同名字段的残留问题
        old_fields = dict()
        for item in ResultTableField.objects.filter(result_table_id=result_table.result_table_id):
            if item.field_name not in old_fields:
                old_fields[item.field_name] = item
            # 若已经出现过同名字段，则其他同名字段均用id记录，防止重复字段的问题被掩盖，最终在旧字段的删除节点清理掉重复字段
            else:
                old_fields[item.id] = item

        # 更新字段信息
        updated_fields = dict()
        for index, field_info in enumerate(fields):
            field_info["field_index"] = field_info.get("field_index", index)
            field_info["result_table_id"] = result_table.result_table_id
            field = ResultTableField(**field_info)
            if field.field_name in old_fields:
                # 防止前端传重复的字段过来导致重复的表字段指数增加
                if field.field_name in updated_fields:
                    continue
                field_info["updated_by"] = bk_username
                old_field = old_fields.pop(field.field_name)
                if ("field_type" in field_info) and field_info["field_type"] != old_field.field_type:
                    raise ResultTableFieldTypeChangedError(message_kv={"field_name": field.field_name})
                old_field.update(field_info)
                updated_fields[field.field_name] = True
            else:
                field.created_by = bk_username
                field.save()
                field_info["created_by"] = bk_username

        # 删除已经不存在的字段
        for field in list(old_fields.values()):
            field.delete()

    @staticmethod
    def delete_table(result_table):
        from meta.public.models.data_processing import DataProcessingRelation

        bk_username = get_request_username()
        fields = ResultTableField.objects.filter(result_table_id=result_table.result_table_id)
        relations = DataProcessingRelation.objects.filter(
            data_directing="input", data_set_type="result_table", data_set_id=result_table.result_table_id
        )

        result_table_content = model_to_dict(result_table)
        result_table_content["fields"] = list(fields.values())
        result_table_content["relations"] = list(relations.values())
        ResultTableDel.objects.create(
            result_table_id=result_table.result_table_id,
            result_table_content=json.dumps(result_table_content, cls=CustomJSONEncoder),
            status="deleted",
            deleted_by=bk_username,
        )

        relations.delete()
        fields.delete()
        result_table.delete()


class ResultTable(AuditMixin):
    result_table_id = models.CharField(
        _("结果表ID"), max_length=255, primary_key=True, error_messages={"unique": _("结果表({result_table_id})已存在")}
    )
    bk_biz_id = models.IntegerField(_("业务ID"))
    project_id = models.IntegerField(_("项目ID"))
    result_table_name = models.CharField(_("结果表名"), max_length=255)
    result_table_name_alias = models.CharField(_("别名"), max_length=255)
    result_table_type = models.IntegerField(_("结果表类型"), null=True, blank=True)
    processing_type = models.CharField(_("数据处理类型"), max_length=32)
    generate_type = models.CharField(_("生成类型"), max_length=32, default="user")
    sensitivity = models.CharField(
        _("敏感性"),
        max_length=32,
        default="private",
        choices=(
            ("public", _("公开")),
            ("private", _("私有")),
            ("sensitive", _("敏感")),
        ),
    )
    count_freq = models.IntegerField(_("统计频率"), default=0)
    count_freq_unit = models.CharField(max_length=32, default="S")
    is_managed = models.IntegerField(_("是否为受控表"), default=1)
    # 非空的字段应在serializer field中，添加默认值。
    platform = models.CharField(_("所属平台"), max_length=32, default="bk_data")
    description = models.TextField(_("备注信息"))
    data_category = models.CharField(max_length=32, default="UTF8")

    objects = ResultTableManager()

    class Meta:
        db_table = "result_table"
        app_label = "public"
        managed = False


class ResultTableDel(DelMixin):
    id = models.AutoField(primary_key=True)
    result_table_id = models.CharField(_("结果表标识"), max_length=255)
    result_table_content = models.TextField(_("结果表的内容"))

    class Meta:
        db_table = "result_table_del"
        app_label = "public"
        managed = False


class JsonActedFieldMixIn(object):
    def get_prep_value(self, value):
        value = super(JsonActedFieldMixIn, self).get_prep_value(value)
        if not isinstance(value, str):
            return json.dumps(value, cls=DjangoJSONEncoder)
        else:
            return value

    def from_db_value(self, value, expression, connection, context):
        if hasattr(super(JsonActedFieldMixIn, self), "from_db_value"):
            value = super(JsonActedFieldMixIn, self).from_db_value(value, expression, connection, context)
        return self.to_python(value)

    @staticmethod
    def to_python(value):
        if value is None:
            return value
        if not isinstance(value, str):
            return value
        return json.loads(value)


class JsonActedCharField(JsonActedFieldMixIn, models.CharField):
    pass


class ResultTableField(AuditMixin):
    id = models.AutoField(_("结果表字段ID"), primary_key=True)
    result_table_id = models.CharField(_("所属结果表"), max_length=255)
    field_index = models.IntegerField(_("字段在数据集中的顺序"))
    field_name = models.CharField(_("字段名"), max_length=255)
    field_alias = models.CharField(_("字段中文名"), max_length=255)
    description = models.TextField(_("描述"), null=True, blank=True)
    field_type = models.CharField(_("数据类型"), max_length=255)
    is_dimension = models.BooleanField(_("是否为维度"), default=False)
    origins = models.CharField(_("原始字段名称"), max_length=1024, null=True, blank=True)
    roles = JsonActedCharField(_("角色"), max_length=255, null=True, blank=True)

    def update(self, info):
        self.field_index = info.get("field_index")
        self.field_alias = info.get("field_alias")
        self.description = info.get("description")
        self.field_type = info.get("field_type")
        self.is_dimension = info.get("is_dimension")
        self.origins = info.get("origins")
        self.roles = info.get("roles")
        self.updated_by = info.get("updated_by")
        return self.save()

    def __eq__(self, other):
        if self.field_name != other.field_name:
            return False
        if self.field_alias != other.field_alias:
            return False
        if self.field_index != other.field_index:
            return False
        if self.is_dimension != other.is_dimension:
            return False

    def __hash__(self):
        return super().__hash__()

    class Meta:
        db_table = "result_table_field"
        app_label = "public"
        managed = False


meta_sync_register(ResultTable)
meta_sync_register(ResultTableField)
