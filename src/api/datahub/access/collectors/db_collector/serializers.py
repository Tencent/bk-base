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

import json
import random
import re

import conf.dataapi_settings as dataapi_settings
import datahub.access.collectors.utils.deploy_plans as deploy_plans_util
import datahub.access.settings as settings
from common.exceptions import ValidationError
from common.log import logger
from datahub.access.collectors.utils.conf_util import get_biz_manager_config
from datahub.access.exceptions import CollectorError, CollerctorCode
from datahub.access.handlers.db_type import DbTypeHandler
from datahub.access.models import AccessHostConfig, AccessManagerConfig
from datahub.access.serializers import BaseSerializer, check_perm_by_scenario
from datahub.access.utils import forms
from django.utils.translation import ugettext as _
from rest_framework import serializers

from ...handlers import GseHandler


class DBResourceSerializer(BaseSerializer):
    class AccessConfSerializer(serializers.Serializer):
        class CollectionModelSerializer(serializers.Serializer):
            collection_type = serializers.ChoiceField(
                required=True,
                label=_("采集方式"),
                choices=(
                    ("all", "全量"),
                    ("time", "时间范围"),
                    ("pri", "主键"),
                ),
            )
            start_at = serializers.IntegerField(required=False, default=0, label=_("是否接入存量数据"))
            time_format = serializers.CharField(required=False, label=_("时间格式"))
            period = serializers.IntegerField(required=True, label=_("采集周期"))
            increment_field = serializers.CharField(required=False, label=_("时间字段"))
            before_time = serializers.IntegerField(required=True, label=_("数据延迟时间"))

            def validate(self, attrs):
                period = attrs["period"]
                before_time = attrs["before_time"]

                if before_time < 0:
                    raise ValidationError(message=_("before_time:延时时间不能小于0"))

                if period <= 0:
                    raise ValidationError(message=_("period采集周期目前只支持周期性拉取"))

                if period > 30 * 24 * 60:
                    raise ValidationError(message=_("period采集周期不能高于30天"))

                collection_type = attrs["collection_type"]
                if collection_type == "pri":
                    increment_field = attrs.get("increment_field")
                    if not increment_field:
                        raise ValidationError(
                            message=_("主键类型时，increment_field必须含有主键"),
                            errors={"increment_field": ["参数必须有"]},
                        )
                elif collection_type == "time":
                    time_format = attrs.get("time_format")
                    increment_field = attrs.get("increment_field")
                    if not time_format or not increment_field:
                        raise ValidationError(
                            message=_("时间范围类型时，increment_field,time_forma必须填写"),
                            errors={"increment_field,time_format": ["参数必须有"]},
                        )
                elif collection_type == "all":
                    pass
                return attrs

        class ResourceScopeSerializer(serializers.Serializer):
            class ScopeSerializer(serializers.Serializer):
                deploy_plan_id = serializers.IntegerField(required=False, label=_("部署计划id"))
                db_host = serializers.CharField(required=True, label=_("接入db的host"))
                db_port = serializers.IntegerField(required=True, label=_("接入db的post"))
                db_user = serializers.CharField(required=True, label=_("接入db的user"))
                db_pass = serializers.CharField(required=True, label=_("接入db的pass"))
                db_name = serializers.CharField(required=True, label=_("接入db的dbnmae"))
                table_name = serializers.CharField(required=True, label=_("接入db的tablename"))
                db_type_id = serializers.IntegerField(required=True, label=_("数据库类型id"))

                def validate_db_host(self, value):
                    re_domain = "[a-zA-Z][-a-zA-Z]{0,62}(.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+.?"
                    if not re.match(re_domain, value) and not forms.check_ip_rule(value):
                        raise ValidationError(message=_("db_host不符合域名和ip规范"))

                    return value

                def validate(self, attrs):
                    db_type_ret = DbTypeHandler(attrs["db_type_id"]).retrieve()
                    if not db_type_ret:
                        raise ValidationError(message=_("DB类型中无可用的数据库类型"))

                    return attrs

            scope = ScopeSerializer(required=True, many=True, label=_("接入对象"))

        class FiltersSerializer(serializers.Serializer):
            key = serializers.CharField(required=True, label=_("key"))
            logic_op = serializers.CharField(required=False, allow_blank=True, label=_("逻辑关系"))
            op = serializers.CharField(required=True, label=_("操作"))
            value = serializers.CharField(required=True, label=_("值"))

            def validate(self, attrs):
                logic_op = attrs.get("logic_op")

                if logic_op:
                    if logic_op.lower() != "and" and logic_op.lower() != "or":
                        raise ValidationError(message=_("参数错误:logic_op 只支持与,或"), errors="logic_op 只支持与,或")
                else:
                    # 默认为and
                    attrs["logic_op"] = "and"

                op = attrs["op"]
                if op != "=" and op != "!=" and op != "in":
                    raise ValidationError(message=_("参数错误:op 只支持等于或者不等于"), errors="op 只支持等于或者不等于")

                return attrs

        collection_model = CollectionModelSerializer(required=True, many=False, label=_("采集方式"))
        resource = ResourceScopeSerializer(required=True, many=False, label=_("接入信息"))
        filters = FiltersSerializer(required=False, many=True, label=_("过滤条件"))

    access_conf_info = AccessConfSerializer(required=True, many=False, label=_("接入配置信息"))

    def check_num_type(self, nums):
        try:
            int(nums)
        except Exception:
            raise ValidationError(message=_("所选字段不是整数类型, 请重新选择"))

    def validate(self, attrs):
        attrs = super(DBResourceSerializer, self).validate(attrs)
        # 验证时间格式与真实数据是否匹配
        host_config = AccessHostConfig.objects.filter(
            data_scenario="db",
            active=1,
            action=dataapi_settings.COLLECTOR_HOST_CONF_ACTION_DEPLOY,
        ).values()

        if not host_config:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_HOST_CONFIG)

        bk_biz_id = get_biz_manager_config()

        manager = AccessManagerConfig.objects.get(type="job").names
        gse = GseHandler(
            task_id=None,
            bk_biz_id=bk_biz_id,
            conf_path=None,
            setup_path=None,
            proc_name=None,
            pid_path=None,
            host_config=host_config,
            bk_username=manager,
        )

        # 随机取1-10随机数,一般情况主机不会多余10,能保证一定随机性
        index = random.randint(1, 10) % len(host_config)
        hosts = [
            {
                "ip": host_config[index]["ip"],  # dataapi_settings.DB_COLLECTOR_DEFAULT_IP
                "bk_cloud_id": host_config[index]["source"],  # dataapi_settings.SERVER_DEFAULT_CLOUD_ID
            }
        ]
        db_array = attrs["access_conf_info"]["resource"]["scope"]
        if len(db_array) > 1:
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_NOT_SUPPORT_MULTI_PLANS,
                errors="not support now",
            )

        data_array = list()
        for db in db_array:
            exec_content = "cd " + dataapi_settings.DB_HOME_PATH + " && "
            exec_content = exec_content + "./dbcheck -h '{}' -P '{}' -u '{}' -p '{}' -D '{}' -t '{}' -r '{}'".format(
                db["db_host"],
                db["db_port"],
                db["db_user"],
                db["db_pass"],
                db["db_name"],
                db["table_name"],
                1,
            )

            exc_result = gse.exc_script(hosts, str(exec_content))

            if exc_result[0]["status"] == deploy_plans_util.TASK_IP_LOG_STATUS.SUCCESS_CODE:
                if exc_result[0]["content"]:
                    if deploy_plans_util.is_json(exc_result[0]["content"]):
                        data_array.extend(json.loads(exc_result[0]["content"]))
                    else:
                        logger.error("error validate db : %s" % exc_result[0]["content"])
                        raise CollectorError(
                            error_code=CollerctorCode.COLLECTOR_VALIDATE_DB_FAIL,
                            errors="validate_error:%s:拉取数据库数据失败" % exc_result,
                        )
            else:
                raise CollectorError(
                    error_code=CollerctorCode.COLLECTOR_CHECK_DB_FAIL,
                    errors="validate_error:%s:拉取数据库列表失败，请检查主机、端口和授权" % exc_result,
                )

        collection_type = attrs["access_conf_info"]["collection_model"]["collection_type"]
        if data_array and collection_type == "time":
            time_format = attrs["access_conf_info"]["collection_model"]["time_format"]
            increment_field = attrs["access_conf_info"]["collection_model"]["increment_field"]
            for data in data_array:
                time_format_re = settings.TIME_FORMAT_REG.get(time_format)
                logger.info(
                    "Time format and time data value, data_increment_field:%s,time_format:%s,time_format_re:%s"
                    % (data[increment_field], time_format, time_format_re)
                )
                if time_format_re and data.get(increment_field) and settings.UNIX_TIME_FORMAT_PREFIX not in time_format:
                    if not re.match(time_format_re, data[increment_field]):
                        raise ValidationError(message=_("time_format时间格式与数据不匹配,请重新选择"))
                elif data.get(increment_field) and settings.UNIX_TIME_FORMAT_PREFIX in time_format_re:
                    # timestamp 类型字段需要校验是否可以转换为long格式
                    self.check_num_type(increment_field)
        elif data_array and collection_type == "incr":
            # id类型字段需要校验是否可以转换为long格式
            increment_field = attrs["access_conf_info"]["collection_model"]["increment_field"]
            self.check_num_type(increment_field)
        return attrs

    @classmethod
    def scenario_scope_check(cls, attrs):
        access_conf_info = attrs.get("access_conf_info", {})
        period = access_conf_info.get("collection_model", {}).get("period")
        before_time = access_conf_info.get("collection_model", {}).get("before_time")

        if before_time:
            if int(before_time) < 0:
                raise ValidationError(message=_("before_time:延时时间不能小于0"))

        if period:
            if int(period) <= 0:
                raise ValidationError(message=_("period采集周期目前只支持周期性拉取"))

            if int(period) > 30 * 24 * 60:
                raise ValidationError(message=_("period采集周期不能高于30天"))


class DBCheckSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(label=_("业务id"))
    bk_username = serializers.CharField(label=_("用户名"))
    db_host = serializers.CharField(label=_("数据库地址"))
    db_port = serializers.IntegerField(label=_("数据库端口"))
    db_user = serializers.CharField(label=_("用户名"))
    db_pass = serializers.CharField(label=_("密码"))
    db_name = serializers.CharField(required=False, allow_blank=True, label=_("数据库名称"))
    tb_name = serializers.CharField(required=False, allow_blank=True, label=_("数据库表"))
    counts = serializers.IntegerField(required=False, label=_("条数"))
    op_type = serializers.ChoiceField(
        required=True,
        allow_blank=True,
        label=_("探测类型"),
        choices=(
            ("db", "db"),
            ("tb", "tb"),
            ("field", "field"),
            ("rows", "rows"),
        ),
    )

    def validate(self, attrs):
        # DB场景请求：biz.common_access 权限
        check_perm_by_scenario("db", attrs["bk_biz_id"], attrs["bk_username"])

        op_type = attrs["op_type"]
        if op_type == "tb":
            db_name = attrs.get("db_name")
            if not db_name or db_name.replace(" ", "") == "":
                raise ValidationError(message=_("请选择db"))
        if op_type == "field":
            db_name = attrs.get("db_name")
            if not db_name or db_name.replace(" ", "") == "":
                raise ValidationError(message=_("请选择db"))

            tb_name = attrs.get("tb_name")
            if not tb_name or tb_name.replace(" ", "") == "":
                raise ValidationError(message=_("请选择表"))
        if op_type == "rows":
            counts = attrs.get("counts")
            if counts:
                if counts < 0:
                    raise ValidationError(message=_("数据条数必须大于0"))
        return attrs
