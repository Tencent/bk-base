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
from __future__ import absolute_import, unicode_literals

import re

from common.auth import check_perm
from common.exceptions import ValidationError
from common.log import logger
from datahub.access.utils import forms
from django.forms import model_to_dict
from django.utils.translation import ugettext as _
from rest_framework import serializers

from ..common.const import ACTION, BK_BIZ_ID, DISABLE, ENABLE
from . import settings
from .collectors.factory import CollectorFactory
from .models import (
    AccessRawData,
    AccessResourceInfo,
    AccessScenarioConfig,
    AccessScenarioStorageChannel,
    AccessSourceConfig,
    EncodingConfig,
)


def check_permissions(raw_data_id):
    """
    检测权限
    :param raw_data_id: dataid
    """
    if raw_data_id:
        raw_data = AccessRawData.objects.filter(id=raw_data_id)
        if raw_data:
            if raw_data[0].permission == "read_only":
                raise ValidationError(message=_("数据只可读，暂不支持更新"))


# 递归遍历数据分类tree
def validate_all_category(data, value):
    if not data["sub_list"]:
        if value == data["category_name"]:
            return True
    for sub_data in data["sub_list"]:
        if validate_all_category(sub_data, value):
            return True

    return False


# 递归遍历数据分类tree
def serializer_all_category(data, map):
    map[data["category_name"]] = data["category_alias"]

    if data["sub_list"]:
        for sub_data in data["sub_list"]:
            map = serializer_all_category(sub_data, map)

    return map


# 递归返回数据分类
def set_subMenus(id, menus):
    """
    :param id: 父id
    :param subMenu:子菜单列表
    :return: 没有子菜单返回None 有子菜单返回子菜单列表
    """
    try:
        _subMenus = []

        for menu in menus:
            if menu.pid == id:
                _subMenus.append(model_to_dict(menu))
        for sub in _subMenus:
            menu2 = AccessSourceConfig.objects.filter(id=sub["id"])
            if len(menus):
                sub["_subMenus"] = set_subMenus(sub["id"], menu2)
            else:
                sub.__delattr__("_subMenus")
        # 子菜单列表不为空
        if len(_subMenus):
            return _subMenus
        else:  # 没有子菜单了
            return []
    except Exception as e:
        logger.exception("error query_sub_menu_info !")
        raise e


def check_perm_by_scenario(scenario, bk_biz_id, username):
    """
    根据场景，验证创建源数据权限和业务权限，"tlog", "log", "script"需要 biz.job_access 权限，其他场景需要biz.common_access权限
    :param username:
    :param scenario: 场景
    :param bk_biz_id: bk_biz_id
    :return
    """
    from .collectors.factory import CollectorFactory

    CollectorFactory.get_collector_factory().get_collector_by_data_scenario(scenario).check_perm_by_scenario(
        bk_biz_id, username
    )


class RawDataSerializer(serializers.Serializer):
    """
    raw_data创建参数校验
    """

    # 数据定义
    bk_biz_id = serializers.IntegerField(label=_("业务"))
    data_source_tags = serializers.ListField(required=False, label=_("数据来源标签"))
    data_source = serializers.CharField(
        required=False,
        max_length=settings.LIMIT_DATA_SOURCE_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("数据来源"),
    )
    tags = serializers.ListField(required=False, label=_("数据标签"))
    bk_app_code = serializers.CharField(
        max_length=settings.LIMIT_BK_APP_CODE_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("接入渠道"),
    )
    storage_channel_id = serializers.IntegerField(required=False, label=_("存储渠道"))
    raw_data_name = serializers.CharField(
        max_length=settings.LIMIT_RAW_DATA_NAME_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("源数据名称"),
    )
    raw_data_alias = serializers.CharField(
        max_length=settings.LIMIT_RAW_DATA_ALIAS_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("源数据中文名称"),
    )
    description = serializers.CharField(required=False, label=_("数据源描述"), allow_blank=True)
    data_encoding = serializers.CharField(
        required=True,
        max_length=settings.LIMIT_DATA_ENCODING_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("字符集编码"),
    )
    data_region = serializers.CharField(required=False, label=_("区域"))
    odm_name = serializers.CharField(required=False, label=_("tglog ODM名"))
    odm_table_name = serializers.CharField(required=False, label=_("tglog ODM名"))

    # 数据权限
    maintainer = serializers.CharField(
        required=True,
        max_length=settings.LIMIT_MAINTAINER_BY_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("数据管理员"),
    )
    # TODO 暂时仅支持 public/private/confidential, 不支持 topsecret
    sensitivity = serializers.ChoiceField(
        required=True,
        label=_("数据敏感度"),
        choices=(
            (settings.SENSITIVITY_PRIVATE_TYPE, "私有"),
            (settings.SENSITIVITY_PUBLIC_TYPE, "公开"),
            (settings.SENSITIVITY_CONFIDENTIAL_TYPE, "机密"),
            # (settings.SENSITIVITY_TOP_SECRET_TYPE, u'绝密'),
        ),
    )

    # 基础信息
    data_scenario = serializers.CharField(
        max_length=settings.LIMIT_DATA_SCENARIO_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("接入场景"),
    )
    bk_username = serializers.CharField(
        required=True,
        max_length=settings.LIMIT_CREATE_BY_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("创建者"),
    )
    created_at = serializers.DateTimeField(required=False, label=_("创建时间"))
    updated_by = serializers.CharField(
        required=False,
        max_length=settings.LIMIT_UPDATE_BY_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("更新者"),
    )
    updated_at = serializers.DateTimeField(required=False, label=_("更新时间"))
    preassigned_data_id = serializers.IntegerField(label=_("预先分配的data_id"), required=False)
    topic_name = serializers.CharField(label=_("预先分配的topic_name"), required=False)

    def validate_sensitivity(self, value):
        return value

    def validate_raw_data_name(self, value):
        # "是否为变量名"
        if not re.search(settings.LIMIT_RAW_DATA_NAME_REG, value):
            raise ValidationError(
                message=_("参数校验不通过:不是合法的命名({})").format(value),
                errors={"raw_data_name": ["不是合法的命名"]},
            )

        return value

    def validate_data_scenario(self, value):
        obj = AccessScenarioConfig.objects.filter(data_scenario_name=value)
        if not obj:
            raise ValidationError(
                message=_("参数校验不通过:接入场景类型不存在({})").format(value),
                errors={"data_scenario": ["接入场景类型不存在"]},
            )

        storage_list = AccessScenarioStorageChannel.objects.filter(data_scenario=value).order_by("-priority")
        if not storage_list:
            raise ValidationError(
                message=_("参数校验不通过:接入场景类型不存在存储配置({})").format(value),
                errors={"data_scenario": ["接入场景类型不存在存储配置"]},
            )

        return value

    def validate_data_encoding(self, value):
        obj = EncodingConfig.objects.filter(encoding_name=value)
        if not obj:
            raise ValidationError(
                message=_("参数校验不通过:字符编码不存在({})").format(value),
                errors={"data_encoding": ["字符编码不存在"]},
            )

        return value

    def validate(self, attrs):
        raw_data_name = attrs["raw_data_name"]
        bk_biz_id = attrs["bk_biz_id"]
        data_scenario = attrs["data_scenario"]

        if data_scenario == "tube":
            existed = AccessRawData.objects.filter(raw_data_name=raw_data_name)
        else:
            existed = AccessRawData.objects.filter(raw_data_name=raw_data_name, bk_biz_id=bk_biz_id)
        if existed:
            raise ValidationError(
                message=_("参数校验不通过:数据源名称已存在,请忽重复提交"),
                errors={"raw_data_name": ["请更换数据源名称或者更新接入"]},
            )

        return attrs


class RawDataUpdateSerializer(serializers.Serializer):
    """
    raw_data更新时参数校验
    """

    # 数据定义
    data_source_tags = serializers.ListField(required=False, label=_("数据来源标签"))
    data_source = serializers.CharField(
        required=False,
        max_length=settings.LIMIT_DATA_SOURCE_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("数据来源"),
    )
    tags = serializers.ListField(required=False, label=_("数据标签"))
    storage_channel_id = serializers.IntegerField(required=False, label=_("存储渠道"))
    raw_data_alias = serializers.CharField(
        max_length=settings.LIMIT_RAW_DATA_ALIAS_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("源数据中文名称"),
    )
    description = serializers.CharField(required=False, label=_("数据源描述"), allow_blank=True)
    data_encoding = serializers.CharField(
        required=True,
        max_length=settings.LIMIT_DATA_ENCODING_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("字符集编码"),
    )
    data_region = serializers.CharField(required=False, label=_("区域"))

    # 数据权限
    maintainer = serializers.CharField(
        required=True,
        max_length=settings.LIMIT_MAINTAINER_BY_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("数据管理员"),
    )
    # TODO 暂时仅支持 public/private/confidential, 不支持 topsecret
    sensitivity = serializers.ChoiceField(
        required=True,
        label=_("数据敏感度"),
        choices=(
            (settings.SENSITIVITY_PRIVATE_TYPE, "私有"),
            (settings.SENSITIVITY_PUBLIC_TYPE, "公开"),
            (settings.SENSITIVITY_CONFIDENTIAL_TYPE, "保密"),
            # (settings.SENSITIVITY_TOP_SECRET_TYPE, u'绝密'),
        ),
    )

    # 基础信息
    bk_username = serializers.CharField(
        required=True,
        max_length=settings.LIMIT_CREATE_BY_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("创建者"),
    )
    updated_by = serializers.CharField(
        required=False,
        max_length=settings.LIMIT_UPDATE_BY_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("更新者"),
    )
    updated_at = serializers.DateTimeField(required=False, label=_("更新时间"))

    def validate_data_encoding(self, value):
        obj = EncodingConfig.objects.filter(encoding_name=value)
        if not obj:
            raise ValidationError(
                message=_("参数校验不通过:字符编码不存在({})").format(value),
                errors={"data_encoding": ["字符编码不存在"]},
            )

        return value


class RetriveSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label=_("源数据ID"), help_text="raw_data_id必须为整型")
    show_display = serializers.IntegerField(required=False, label=_("是否显示源数据"), help_text="show_display必须为整型")
    bk_username = serializers.CharField(required=False, label=_("访问用户"))


class DataIdSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label=_("源数据ID"), help_text="raw_data_id必须为整形")


class DataNameSerializer(serializers.Serializer):
    raw_data_name = serializers.CharField(label=_("源数据名称"))
    bk_biz_id = serializers.IntegerField(label=_("业务id"))


class DataIdReqSerializer(serializers.Serializer):
    data_id = serializers.IntegerField(required=False, label=_("源数据ID"))


class AuthTicketsCallbackSerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=True, label=_("用户名"))
    status = serializers.CharField(required=True, label=_("状态"))
    process_id = serializers.IntegerField(required=True, label=_("处理ID"))


class DataIdKafkaConfSerializer(serializers.Serializer):
    data_id = serializers.IntegerField(label=_("源数据ID"))
    kafka_cluster_id = serializers.IntegerField(label=_("kafka集群ID"))


class DataScenarioSerializer(serializers.Serializer):
    data_scenario = serializers.CharField(
        max_length=settings.LIMIT_DATA_SCENARIO_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("接入场景"),
    )

    def validate_data_scenario(self, value):
        obj = AccessScenarioConfig.objects.filter(data_scenario_name=value)
        if not obj:
            raise ValidationError(
                message=_("参数校验不通过:接入场景类型不存在({})").format(value),
                errors={"data_scenario": ["接入场景类型不存在"]},
            )

        return value


class ListPageByParamSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(required=False, label=_("业务"))

    show_display = serializers.BooleanField(required=False, default=False, label=_("是否显示"))

    raw_data_name__icontains = serializers.CharField(required=False, label=_("源数据名称"))

    raw_data_id__icontains = serializers.IntegerField(required=False, label=_("源数据ID"))

    data_scenario = serializers.CharField(
        required=False,
        max_length=settings.LIMIT_DATA_SCENARIO_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("接入场景"),
    )
    data_source = serializers.CharField(
        required=False,
        max_length=settings.LIMIT_DATA_SOURCE_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("数据来源"),
    )

    page = serializers.IntegerField(required=False, label=_("当前页"))
    page_size = serializers.IntegerField(required=False, label=_("页数"))

    created_by = serializers.CharField(required=False, label=_("创建人"))
    created_begin = serializers.CharField(required=False, label=_("创建起始时间"))
    created_end = serializers.CharField(required=False, label=_("创建结束时间"))

    def validate(self, attrs):
        page_size = attrs.get("page_size")
        page = attrs.get("page")
        if page_size and page_size > 100:
            raise ValidationError(
                message=_("参数校验不通过:page_size必须小于100,({})").format(page_size),
                errors={"page_size": ["必须小于100"]},
            )

        if page and page < 1:
            raise ValidationError(
                message=_("参数校验不通过:page必须大于0,({})").format(page_size),
                errors={"page": ["page必须大于0"]},
            )

        if (not page_size or not page) and not attrs.get(BK_BIZ_ID):
            raise ValidationError(
                message=_("参数校验不通过: bk_biz_id或者page_size、page至少填写一类"),
                errors={"page_size": ["参数校验不通过: bk_biz_id或者page_size、page至少填写一类"]},
            )

        attrs["active"] = 1

        created_begin = attrs.get("created_begin")
        if created_begin:
            created_end = attrs.get("created_end")
            if not created_end:
                raise ValidationError(message="missing created_end")

        return attrs


class ListByParamSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(required=False, label=_("业务"))

    show_display = serializers.BooleanField(required=False, default=False, label=_("是否显示"))
    show_biz_name = serializers.BooleanField(required=False, default=True, label=_("是否显示业务名"))

    raw_data_name__icontains = serializers.CharField(required=False, label=_("源数据名称"))

    raw_data_id__icontains = serializers.IntegerField(required=False, label=_("源数据ID"))

    data_scenario = serializers.CharField(
        required=False,
        max_length=settings.LIMIT_DATA_SCENARIO_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("接入场景"),
    )
    data_source = serializers.CharField(
        required=False,
        max_length=settings.LIMIT_DATA_SOURCE_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("数据来源"),
    )

    page = serializers.IntegerField(required=False, label=_("当前页"))
    page_size = serializers.IntegerField(required=False, label=_("页数"))

    def validate(self, attrs):
        page_size = attrs.get("page_size")
        page = attrs.get("page")
        if page_size and page_size > 100:
            raise ValidationError(
                message=_("参数校验不通过:page_size必须小于100,({})").format(page_size),
                errors={"page_size": ["必须小于100"]},
            )

        if page and page < 1:
            raise ValidationError(
                message=_("参数校验不通过:page必须大于0,({}))").format(page_size),
                errors={"page": ["page必须大于0"]},
            )

        attrs["active"] = 1
        return attrs


# 接入采集校验器-collector


class BaseSerializer(serializers.Serializer):
    class AccessRawDataSerializer(serializers.Serializer):
        raw_data_name = serializers.CharField(label=_("源数据名称"))
        raw_data_alias = serializers.CharField(label=_("源数据名称"))
        data_source_tags = serializers.ListField(required=False, label=_("数据来源标签"))
        data_source = serializers.CharField(required=False, label=_("数据来源"))
        sensitivity = serializers.CharField(label=_("是否为敏感数据"))
        data_encoding = serializers.CharField(label=_("字符编码"))
        tags = serializers.ListField(required=False, label=_("数据区域"))
        data_region = serializers.CharField(required=False, label=_("数据区域"))
        storage_channel_id = serializers.IntegerField(required=False, label=_("存储渠道"))
        maintainer = serializers.CharField(required=True, label=_("维护者"))
        description = serializers.CharField(required=False, label=_("数据源描述"), allow_blank=True)
        preassigned_data_id = serializers.IntegerField(required=False, label=_("预分配的data_id"))
        topic_name = serializers.CharField(required=False, label=_("预分配的topic_name"))

    bk_biz_id = serializers.IntegerField(required=True, label=_("业务"))
    bk_app_code = serializers.CharField(required=True, label=_("接入来源APP"))
    bk_username = serializers.CharField(required=True, label=_("创建者"))
    data_scenario = serializers.CharField(required=True, label=_("接入场景"))
    description = serializers.CharField(required=False, label=_("接入备注"), allow_blank=True)
    access_raw_data = AccessRawDataSerializer(required=True, many=False, label=_("deploy_plans配置"))
    appenv = serializers.CharField(required=False, label=_("接入环境"))
    access_conf_info = serializers.DictField(required=False, label=_("接入配置信息"))

    def validate(self, attrs):
        access_raw_data = attrs["access_raw_data"]
        access_raw_data["bk_biz_id"] = attrs["bk_biz_id"]
        access_raw_data["bk_app_code"] = attrs["bk_app_code"]
        access_raw_data["created_by"] = attrs["bk_username"]
        access_raw_data["data_scenario"] = attrs["data_scenario"]
        # access_raw_data['description'] = attrs.get('description', '')
        # 根据场景，验证创建源数据权限和业务权限
        check_perm_by_scenario(attrs["data_scenario"], access_raw_data["bk_biz_id"], attrs["bk_username"])
        return attrs

    @classmethod
    def scenario_scope_check(cls, attrs):
        """用来对于此场景进行特殊的权限校验"""
        pass


class UpdateBaseSerializer(BaseSerializer):
    """
    重新接入参数校验
    """

    raw_data_id = serializers.IntegerField(required=True, label=_("源数据id"))

    def validate(self, attrs):
        access_raw_data = attrs["access_raw_data"]
        access_raw_data["bk_biz_id"] = attrs["bk_biz_id"]
        access_raw_data["bk_app_code"] = attrs["bk_app_code"]
        access_raw_data["created_by"] = attrs["bk_username"]
        access_raw_data["data_scenario"] = attrs["data_scenario"]
        # access_raw_data['description'] = attrs.get('description', '')
        # 根据场景，验证创建源数据权限和业务权限
        logger.info("base: start verifying permission information")
        check_perm_by_scenario(attrs["data_scenario"], access_raw_data["bk_biz_id"], attrs["bk_username"])

        # 验证数据源权限特性是否为read_ony
        raw_data_id = attrs["raw_data_id"]
        check_permissions(raw_data_id)

        # 特殊场景需要特殊验证
        data_scenario = attrs["data_scenario"]
        from .collectors.factory import CollectorFactory

        CollectorFactory.get_collector_by_data_scenario(data_scenario).scenario_scope_validate(attrs)

        return attrs


class CollectDeleteSerializer(serializers.Serializer):
    """
    采集删除校验
    """

    raw_data_id = serializers.IntegerField(required=False, label=_("源数据id"))
    bk_biz_id = serializers.IntegerField(required=True, label=_("业务"))
    bk_app_code = serializers.CharField(required=True, label=_("接入来源APP"))
    bk_username = serializers.CharField(required=True, label=_("创建者"))
    appenv = serializers.CharField(required=False, label=_("接入环境"))
    force = serializers.BooleanField(required=False, label=_("是否强制删除"), default=False)

    def validate(self, attrs):
        # 验证创建源数据权限和业务权限
        # 根据场景，验证创建源数据权限和业务权限
        data_scenario = AccessRawData.objects.get(id=attrs["raw_data_id"]).data_scenario
        check_perm_by_scenario(data_scenario, attrs["bk_biz_id"], attrs["bk_username"])
        return attrs


class CallBackSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数将在这里定义
    """

    class DeployPlansSerializer(serializers.Serializer):
        class ConfigSerializer(serializers.Serializer):
            raw_data_id = serializers.IntegerField(required=True, label=_("源数据id"))
            deploy_plan_id = serializers.ListField(required=True, label=_("部署计划id"))

        class HostSerializer(serializers.Serializer):
            bk_cloud_id = serializers.IntegerField(required=True, label=_("云区域id"))
            ip = serializers.CharField(required=True, label=_("ip"))

            def validate_ip(self, value):
                if forms.check_ip_rule(value):
                    return value
                logger.error("validate_ip failed, wrong ip: %s" % value)
                raise ValidationError(message=_("ip格式错误"))

        config = ConfigSerializer(required=False, allow_null=True, many=True, label=_("配置"))
        host_list = HostSerializer(required=True, many=True, label=_("host对象"))
        system = serializers.ChoiceField(
            required=True,
            label=_("操作系统"),
            choices=(
                ("linux", "linux"),
                ("windows", "windows"),
                ("unknown", "unknown"),
            ),
        )

    bk_biz_id = serializers.IntegerField(label=_("业务id"))
    bk_username = serializers.CharField(label=_("操作人"))
    version = serializers.CharField(required=False, label=_("采集器版本"))
    deploy_plans = DeployPlansSerializer(required=True, many=True, label=_("deploy_plans配置"))


class DeployPlanScopeSerializer(serializers.Serializer):
    host_list = serializers.ListField(required=True, label=_("部署host列表"))
    deploy_plan_ids = serializers.ListField(required=True, label=_("部署计划ID列表"))

    def validate(self, attrs):
        deploy_plan_ids = attrs["deploy_plan_ids"]
        access_infos = AccessResourceInfo.objects.filter(pk__in=deploy_plan_ids)

        deploy_plan_id_scope_map = {_access_info.id: _access_info.resource for _access_info in access_infos}
        for _deploy_plan_id in deploy_plan_ids:
            if deploy_plan_id_scope_map.get(_deploy_plan_id) is None:
                raise ValidationError(message=_("({}):该部署计划不存在").format(_deploy_plan_id))
        return attrs


class BkBizIdSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(required=True, label=_("业务ID"))


class UserSerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=True, label=_("bk_username"))


class FileUploadSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(required=True, label=_("业务ID"))
    file_data = serializers.FileField(required=True, label=_("文件"))
    bk_username = serializers.CharField(required=True, label=_("bk_username"))

    def validate(self, attrs):
        # check_perm('biz.manage', attrs['bk_biz_id'])
        file_data = attrs["file_data"]
        file_data_name = file_data.name

        if (
            not file_data_name.endswith(".csv")
            and not file_data_name.endswith(".xls")
            and not file_data_name.endswith(".xlsx")
        ):
            raise ValidationError(message=_("目前只支持.csv,.xls,.xlsx 文件"))
        return attrs


class ServiceDeploySerializer(serializers.Serializer):
    class ScopeSerializer(serializers.Serializer):
        class HostScopejSerializer(serializers.Serializer):
            bk_cloud_id = serializers.IntegerField(required=True, label=_("云区域id"))
            ip = serializers.CharField(required=True, label=_("ip"))

            def validate_ip(self, value):
                if forms.check_ip_rule(value):
                    return value
                raise ValidationError(message=_("ip格式错误"))

        class ModuleScopejSerializer(serializers.Serializer):
            bk_obj_id = serializers.CharField(required=True, label=_("接入对象ID"))
            bk_inst_id = serializers.IntegerField(required=True, label=_("bk_inst_id"))

        deploy_plan_id = serializers.IntegerField(required=True, label=_("部署计划ID"))
        hosts = HostScopejSerializer(required=False, many=True, label=_("host_scope配置"))
        modules = ModuleScopejSerializer(required=False, many=True, label=_("modules配置"))

    data_scenario = serializers.CharField(required=True, label=_("接入场景"))
    bk_username = serializers.CharField(required=True, label=_("用户名"))
    raw_data_id = serializers.IntegerField(required=True, label=_("源数据ID"))
    bk_biz_id = serializers.CharField(required=True, label=_("业务ID"))
    config_mode = serializers.ChoiceField(
        required=False,
        label=_("配置方式"),
        choices=(
            ("full", "全量"),
            ("incr", "增量"),
        ),
    )
    scope = ScopeSerializer(allow_empty=True, required=False, many=True, label=_("scope配置"))

    def validate(self, attrs):
        raw_data_id = attrs["raw_data_id"]
        check_perm("raw_data.collect_hub", raw_data_id)
        return attrs


class CollectorHubDeployPlanSerializer(serializers.Serializer):
    class CollectorHubScopeSerializer(serializers.Serializer):
        class HostScopejSerializer(serializers.Serializer):
            bk_cloud_id = serializers.IntegerField(required=True, label=_("云区域id"))
            ip = serializers.CharField(required=True, label=_("ip"))

            def validate_ip(self, value):
                if forms.check_ip_rule(value):
                    return value
                raise ValidationError(message=_("ip格式错误"))

        class ModuleScopejSerializer(serializers.Serializer):
            bk_obj_id = serializers.CharField(required=True, label=_("接入对象ID"))
            bk_inst_id = serializers.IntegerField(required=True, label=_("bk_inst_id"))

        deploy_plan_id = serializers.IntegerField(required=True, label=_("部署计划ID"))
        hosts = HostScopejSerializer(allow_empty=True, required=False, many=True, label=_("host_scope配置"))
        modules = ModuleScopejSerializer(allow_empty=True, required=False, many=True, label=_("modules配置"))

    data_scenario = serializers.CharField(required=True, label=_("接入场景"))
    bk_username = serializers.CharField(required=True, label=_("用户名"))
    raw_data_id = serializers.IntegerField(required=True, label=_("源数据ID"))
    bk_biz_id = serializers.CharField(required=True, label=_("业务ID"))
    config_mode = serializers.ChoiceField(
        required=True,
        label=_("配置方式"),
        choices=(
            ("full", "全量"),
            ("incr", "增量"),
        ),
    )
    scope = CollectorHubScopeSerializer(allow_empty=False, required=True, many=True, label=_("scope配置"))

    def validate(self, attrs):
        raw_data_id = attrs["raw_data_id"]
        data_scenario = attrs["data_scenario"]
        check_perm("raw_data.collect_hub", raw_data_id)
        from .collectors.factory import CollectorFactory

        bk_biz_id, bk_username = CollectorFactory.get_collector_by_data_scenario(data_scenario).scenario_biz_info(
            attrs["bk_biz_id"], attrs["bk_username"]
        )
        attrs["bk_biz_id"] = bk_biz_id
        attrs["bk_username"] = bk_username
        return attrs


class CollectorHubQueryHistorySerializer(serializers.Serializer):

    bk_username = serializers.CharField(required=True, label=_("用户名"))
    raw_data_id = serializers.IntegerField(required=True, label=_("源数据ID"))
    ip = serializers.CharField(required=True, label=_("ip"))
    bk_cloud_id = serializers.IntegerField(required=True, label=_("云平台ID"))

    def validate_ip(self, value):
        if forms.check_ip_rule(value):
            return value
        raise ValidationError(message=_("ip格式错误"))


class CollectorHubQueryStatusSerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=True, label=_("用户名"))
    raw_data_id = serializers.IntegerField(required=True, label=_("源数据ID"))
    ip = serializers.CharField(required=False, label=_("ip"))
    bk_cloud_id = serializers.IntegerField(required=False, label=_("云平台ID"))
    ordering = serializers.CharField(required=False, label=_("按照关键字排序"))
    descending = serializers.IntegerField(required=False, label=_("排序"))
    deploy_status = serializers.CharField(required=False, label=_("部署状态"))
    page = serializers.IntegerField(required=False, label=_("页码"))
    page_size = serializers.IntegerField(required=False, label=_("页码"))

    def validate_ip(self, value):
        if forms.check_ip_rule(value):
            return value
        raise ValidationError(message=_("ip格式错误"))


class CollectorHubSumarySerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=True, label=_("用户名"))
    deploy_plan_id = serializers.IntegerField(required=False, label=_("部署计划ID"))
    bk_obj_id = serializers.CharField(required=False, label=_("配置平台对象ID"))
    bk_inst_id = serializers.IntegerField(required=False, label=_("配置平台实例ID"))


class AccessTaskLogSerializer(serializers.Serializer):
    bk_username = serializers.CharField(required=True, label=_("用户名"))
    raw_data_id = serializers.IntegerField(required=True, label=_("源数据ID"))
    ip = serializers.CharField(required=False, label=_("ip"))
    bk_cloud_id = serializers.IntegerField(required=False, label=_("云平台ID"))

    def validate_ip(self, value):
        if forms.check_ip_rule(value):
            return value
        raise ValidationError(message=_("ip格式错误"))


class RawDataPermissionSerializer(serializers.Serializer):

    raw_data_id = serializers.IntegerField(label=_("源数据id"))
    data_scenario = serializers.CharField(required=False, label=_("接入场景"))
    access_conf_info = serializers.DictField(required=False, label=_("接入配置信息"))
    scope_check = serializers.BooleanField(required=False, label=_("scope验证"))
    bk_username = serializers.CharField(required=False, label=_("创建者"))

    def validate(self, attrs):
        # 验证数据源权限特性是否为read_ony
        check_permissions(attrs["raw_data_id"])

        # 特殊场景需要特殊验证
        if attrs.get("scope_check", False):
            data_scenario = attrs["data_scenario"]
            collector_factory = CollectorFactory.get_collector_factory()
            scenario_serializer = collector_factory.get_collector_by_data_scenario(data_scenario).access_serializer
            scenario_serializer.scenario_scope_check(attrs)

        return attrs


class RawDataRestartSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(required=True, label=_("业务id"))
    raw_data_ids = serializers.ListField(required=True, label=_("raw_data_id列表"))
    bk_username = serializers.CharField(required=True, label=_("创建者"))
    use_v2_unifytlogc = serializers.BooleanField(required=False, label=_("使用v2版unifytlogc, 转换云区域为v3"), default=False)
    only_stop_collector_manage = serializers.BooleanField(required=False, label=_("停止托管，删除采集计划"), default=False)
    only_stop_collector = serializers.BooleanField(required=False, label=_("仅停止任务，不启动任务"), default=False)
    only_start_collector = serializers.BooleanField(required=False, label=_("仅启动任务"), default=False)

    def validate(self, attrs):
        true_count = 0
        if attrs.get("only_stop_collector", False):
            true_count = true_count + 1
        if attrs.get("only_start_collector", False):
            true_count = true_count + 1
        if attrs.get("only_stop_collector_manage", False):
            true_count = true_count + 1
        if true_count > 1:
            raise ValidationError(
                message=_("only_stop_collector、only_start_collector、only_stop_collector_manage" "最多只能配置一个为True")
            )
        return attrs


class DataStructSerializer(serializers.Serializer):
    struct_name = serializers.CharField(required=True, label=_("表名"))


class DataStructSerializerWithXml(serializers.Serializer):
    struct_name = serializers.CharField(required=True, label=_("表名"))
    name = serializers.CharField(required=True, label=_("tlog业务ID"))
    special_filename = serializers.CharField(required=False, label=_("tlog业务ID"))


class TdwTidListSerializer(serializers.Serializer):
    topic = serializers.CharField(required=True, label=_("tdw topic"))


class EnableV2UnifytlogcSerializer(serializers.Serializer):
    raw_data_list = serializers.ListField(required=True, label=_("需要操作的数据源id"))
    action = serializers.CharField(required=True, label=_("需要操作的行为"))

    def validate(self, attrs):
        if attrs.get(ACTION) not in [ENABLE, DISABLE]:
            raise ValidationError(message=_("参数校验错误，目前仅支持enable和disable操作"))
        return attrs


class FilePartUploadSerializer(serializers.Serializer):
    task_name = serializers.CharField(required=True, label=_("文件名"))
    chunk_id = serializers.CharField(required=True, label=_("分片id"))
    bk_biz_id = serializers.IntegerField(required=True, label=_("业务id"))
    raw_data_id = serializers.CharField(required=False, label=_("用来校验是否可以更新"))
    upload_uuid = serializers.CharField(required=True, label=_("用来区分每个上传的文件"))

    def validate(self, attrs):
        return attrs


class FileMergeUploadSerializer(serializers.Serializer):
    task_name = serializers.CharField(required=True, label=_("文件名"))
    chunk_size = serializers.IntegerField(required=True, label=_("分片数量"))
    bk_biz_id = serializers.IntegerField(required=True, label=_("业务id"))
    md5 = serializers.CharField(required=True, label=_("md5校验"))
    upload_uuid = serializers.CharField(required=True, label=_("用来区分每个上传的文件"))


class FileHdfsInfoSerializer(serializers.Serializer):
    file_name = serializers.CharField(required=True, label=_("文件名"))


class DeployPlanListByParamSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(required=False, label=_("业务id"))
    data_scenario = serializers.CharField(
        required=False,
        max_length=settings.LIMIT_DATA_SCENARIO_LEN,
        min_length=settings.MIN_RAW_DATA_NAME_LEN,
        label=_("接入场景"),
    )
    raw_data_ids = serializers.ListField(required=True, label=_("raw_data_id数组"))


class AddStorageChannelSerializer(serializers.Serializer):
    dimension_type = serializers.CharField(required=True, label=_("dimension_type"))
    cluster_type = serializers.CharField(required=True, label=_("cluster_type"))
    bk_username = serializers.CharField(required=True, label=_("bk_username"))
