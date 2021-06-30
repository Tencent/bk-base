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
import datetime
import re

from django.utils.translation import ugettext as _
from rest_framework import serializers
from django.conf import settings
from resourcecenter.handlers import resource_cluster_config

from common.exceptions import ValidationError


class CreateResourceGroupInfoSerializer(serializers.Serializer):
    resource_group_id = serializers.CharField(label=_("资源组英文标识"))
    group_name = serializers.CharField(label=_("资源组名称"))
    admin = serializers.ListField(child=serializers.CharField(), label=_("资源组管理员"))
    group_type = serializers.CharField(label=_("资源组类型"))
    bk_biz_id = serializers.IntegerField(label=_("任务名称"))
    description = serializers.CharField(required=False, allow_blank=True, label=_("描述说明"))

    def validate_resource_group_id(self, resource_group_id):
        if not re.match("^[A-Za-z_]+[A-Za-z_0-9\\-]*$", resource_group_id):
            raise ValidationError(_("资源组英文标识只能包含英文、数字、中划线和下划线，且不能中划线和数字开头"))
        if "__" in resource_group_id:
            raise ValidationError(_("资源组英文标识不允许使用连续两个下划线。"))
        return resource_group_id

    def validate_group_type(self, group_type):
        group_type_arrays = ["public", "protected", "private"]
        if group_type not in group_type_arrays:
            raise ValidationError(
                _("非法资源组类型（%(group_type)s），目前仅支持文件类型（%(group_type_arrays)s）")
                % {"group_type": group_type, "group_type_arrays": ", ".join(group_type_arrays)}
            )
        return group_type


class UpdateResourceGroupInfoSerializer(serializers.Serializer):
    group_name = serializers.CharField(label=_("资源组名称"))
    group_type = serializers.CharField(label=_("资源组类型"))
    bk_biz_id = serializers.IntegerField(label=_("任务名称"))
    description = serializers.CharField(required=False, allow_blank=True, label=_("描述说明"))

    def validate_group_type(self, group_type):
        group_type_arrays = ["public", "protected", "private"]
        if group_type not in group_type_arrays:
            raise ValidationError(
                _("非法资源组类型（%(group_type)s），目前仅支持文件类型（%(group_type_arrays)s）")
                % {"group_type": group_type, "group_type_arrays": ", ".join(group_type_arrays)}
            )
        return group_type


class PartialUpdateResourceGroupInfoSerializer(serializers.Serializer):
    resource_group_id = serializers.CharField(label=_("资源组英文标识"))
    group_name = serializers.CharField(label=_("资源组名称"))
    description = serializers.CharField(required=False, allow_blank=True, label=_("描述说明"))


class CommonApproveResultSerializer(serializers.Serializer):
    operator = serializers.CharField(label=_("审批人"))
    status = serializers.CharField(label=_("审批结果"))


class CreateResourcesGroupCapacityApplyFormSerializer(serializers.Serializer):
    resource_group_id = serializers.CharField(label=_("资源组英文标识"))
    resource_type = serializers.CharField(label=_("资源分类"))
    service_type = serializers.CharField(label=_("服务类型"))
    apply_type = serializers.CharField(label=_("单据类型"))
    geog_area_code = serializers.CharField(label=_("地区"))
    resource_unit_id = serializers.IntegerField(label=_("资源套餐ID"))
    num = serializers.IntegerField(label=_("数量"))
    description = serializers.CharField(label=_("申请原因"))

    def validate_num(self, num):
        if num <= 0:
            raise ValidationError(_("申请资源数量必须大于0。"))
        return num


class PartialUpdateResourcesGroupCapacityApplyFormSerializer(serializers.Serializer):
    apply_id = serializers.IntegerField(label=_("申请单号"))
    resource_unit_id = serializers.IntegerField(label=_("资源套餐ID"))
    num = serializers.IntegerField(label=_("数量"))
    operate_result = serializers.CharField(label=_("实施结果"))
    status = serializers.CharField(label=_("状态"))
    cluster_id = serializers.CharField(label=_("集群ID"))


class CreateResourceUnitConfigSerializer(serializers.Serializer):
    name = serializers.CharField(label=_("配置单元名称"))
    resource_type = serializers.CharField(label=_("资源分类"))
    service_type = serializers.CharField(label=_("服务类型"))
    cpu = serializers.FloatField(required=False, label=_("CPU数量"))
    memory = serializers.FloatField(required=False, label=_("内存数据量"))
    gpu = serializers.FloatField(required=False, label=_("GPU数量"))
    disk = serializers.FloatField(required=False, label=_("存储容量"))
    net = serializers.FloatField(required=False, label=_("网络带宽"))
    slot = serializers.FloatField(required=False, label=_("处理槽位"))
    active = serializers.IntegerField(label=_("是否生效"))


class ResourceServiceConfigSerializer(serializers.Serializer):
    resource_type = serializers.CharField(label=_("资源分类"))
    service_type = serializers.CharField(label=_("服务类型"))
    service_name = serializers.CharField(label=_("服务类型名称"))
    active = serializers.IntegerField(label=_("是否生效"))


class GetResourceServiceConfigSerializer(serializers.Serializer):
    resource_type = serializers.CharField(label=_("资源分类"))


class ResourceClusterConfigSerializer(serializers.Serializer):
    cluster_id = serializers.CharField(label=_("集群ID"))
    cluster_type = serializers.CharField(label=_("集群类型"))
    cluster_name = serializers.CharField(allow_null=True, label=_("集群名称"))
    component_type = serializers.CharField(label=_("组件类型"))
    geog_area_code = serializers.CharField(label=_("地区"))
    resource_group_id = serializers.CharField(label=_("资源组ID"))
    resource_type = serializers.CharField(label=_("资源分类"))
    service_type = serializers.CharField(label=_("服务类型"))
    src_cluster_id = serializers.CharField(label=_("外部系统集群ID"))
    active = serializers.IntegerField(label=_("是否生效"))
    splitable = serializers.CharField(label=_("是否可以拆分分配"))
    cpu = serializers.FloatField(label=_("CPU数量"))
    memory = serializers.FloatField(label=_("内存数据量"))
    gpu = serializers.FloatField(label=_("GPU数量"))
    disk = serializers.FloatField(label=_("存储容量"))
    net = serializers.FloatField(label=_("网络带宽"))
    slot = serializers.FloatField(label=_("处理槽位"))
    available_cpu = serializers.FloatField(label=_("可用CPU数量"))
    available_memory = serializers.FloatField(label=_("可用内存数据量"))
    available_gpu = serializers.FloatField(label=_("可用GPU数量"))
    available_disk = serializers.FloatField(label=_("可用存储容量"))
    available_net = serializers.FloatField(label=_("可用网络带宽"))
    available_slot = serializers.FloatField(label=_("可用处理槽位"))
    connection_info = serializers.CharField(required=False, allow_blank=True, allow_null=True, label=_("集群连接信息"))
    priority = serializers.IntegerField(required=False, allow_null=True, label=_("优先度"))
    belongs_to = serializers.CharField(required=False, allow_blank=True, allow_null=True, label=_("所属父集群"))
    description = serializers.CharField(label=_("描述说明"))

    def validate_belongs_to(self, belongs_to):
        if belongs_to:
            parent_cluster = resource_cluster_config.filter_list(cluster_id=belongs_to)
            if not parent_cluster or len(parent_cluster) <= 0:
                raise ValidationError("父集群:%s不存在" % belongs_to)
        return belongs_to


class ResourceGroupGeogBranchSerializer(serializers.Serializer):
    resource_group_id = serializers.CharField(label=_("资源组ID"))
    geog_area_code = serializers.CharField(label=_("地区"))
    cluster_group = serializers.CharField(label=_("集群组"))


class GetResourceGroupGeogBranchSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(label=_("地区"))


class ProcessingMetricsSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(label=_("地区"))
    service_type = serializers.CharField(label=_("服务类型"))
    start_time = serializers.CharField(label=_("开始时间"))
    end_time = serializers.CharField(label=_("结束时间"))
    time_unit = serializers.CharField(label=_("时间单位"))
    cluster_id = serializers.CharField(required=False, label=_("集群ID"))

    def validate_time_unit(self, time_unit):
        if time_unit not in ["5min", "10min", "30min", "hour", "day"]:
            raise ValidationError(_("不支持的time_unit值，time_unit取值为：5min, 10min, 30min, hour, day"))
        return time_unit


class GetProcessingServiceClusterSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=False, label=_("地区"))
    service_type = serializers.CharField(label=_("服务类型"))


class StorageMetricsSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(label=_("地区"))
    service_type = serializers.CharField(label=_("服务类型"))
    start_time = serializers.CharField(label=_("开始时间"))
    end_time = serializers.CharField(label=_("结束时间"))
    time_unit = serializers.CharField(label=_("时间单位"))
    cluster_id = serializers.CharField(required=False, label=_("集群ID"))


class GetStorageServiceClusterSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=False, label=_("地区"))
    service_type = serializers.CharField(label=_("服务类型"))


class DatabusMetricsSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(label=_("地区"))
    service_type = serializers.CharField(label=_("服务类型"))
    start_time = serializers.CharField(label=_("开始时间"))
    end_time = serializers.CharField(label=_("结束时间"))
    time_unit = serializers.CharField(label=_("时间单位"))
    cluster_id = serializers.CharField(required=False, label=_("集群ID"))


class GetDatabusServiceClusterSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=False, label=_("地区"))
    service_type = serializers.CharField(label=_("服务类型"))


class CreateClusterRegisterSerializer(serializers.Serializer):
    resource_type = serializers.CharField(label=_("资源分类"))
    service_type = serializers.CharField(label=_("服务类型"))
    src_cluster_id = serializers.CharField(label=_("集群ID"))
    cluster_type = serializers.CharField(label=_("集群类型"))
    cluster_name = serializers.CharField(allow_null=True, label=_("集群名称"))
    component_type = serializers.CharField(label=_("组件类型"))
    geog_area_code = serializers.CharField(label=_("区域"))
    resource_group_id = serializers.CharField(required=False, label=_("资源组ID"))
    cluster_group = serializers.CharField(required=False, label=_("集群组"))
    active = serializers.IntegerField(required=False, label=_("是否生效"))
    splitable = serializers.CharField(required=False, label=_("是否可以拆分分配"))
    cpu = serializers.FloatField(required=False, label=_("CPU数量，单位core"))
    memory = serializers.FloatField(required=False, label=_("内存数据量，单位MB"))
    gpu = serializers.FloatField(required=False, label=_("GPU数量，单位core类型"))
    disk = serializers.FloatField(required=False, label=_("存储容量，单位MB"))
    net = serializers.FloatField(required=False, label=_("网络带宽，单位bit"))
    slot = serializers.FloatField(required=False, label=_("处理槽位，单位个"))
    connection_info = serializers.CharField(required=False, allow_blank=True, allow_null=True, label=_("集群连接信息"))
    priority = serializers.IntegerField(required=False, allow_null=True, label=_("优先度"))
    belongs_to = serializers.CharField(required=False, allow_blank=True, allow_null=True, label=_("所属父集群"))

    def validate_resource_type(self, resource_type):
        resource_type_arrays = ["processing", "storage", "databus", "schedule"]
        if resource_type not in resource_type_arrays:
            raise ValidationError(
                _("非法资源分类: %(resource_type)s，目前仅支持文件类型（%(resource_type_arrays)s）")
                % {"resource_type": resource_type, "resource_type_arrays": ", ".join(resource_type_arrays)}
            )
        return resource_type

    def validate_belongs_to(self, belongs_to):
        if belongs_to:
            parent_cluster = resource_cluster_config.filter_list(cluster_id=belongs_to)
            if not parent_cluster or len(parent_cluster) <= 0:
                raise ValidationError("父集群:%s不存在" % belongs_to)
        return belongs_to

    def validate(self, attrs):
        if "resource_group_id" not in attrs and "cluster_group" not in attrs:
            raise ValidationError(_("resource_group_id 和 cluster_group 必须填一个"))

        return attrs


class UpdateClusterRegisterSerializer(serializers.Serializer):
    resource_type = serializers.CharField(label=_("资源分类"))
    service_type = serializers.CharField(label=_("服务类型"))
    src_cluster_id = serializers.CharField(label=_("集群ID"))
    cluster_type = serializers.CharField(label=_("集群类型"))
    cluster_name = serializers.CharField(allow_null=True, label=_("集群名称"))
    component_type = serializers.CharField(label=_("组件类型"))
    geog_area_code = serializers.CharField(label=_("区域"))
    resource_group_id = serializers.CharField(required=False, label=_("资源组ID"))
    cluster_group = serializers.CharField(required=False, label=_("集群组"))
    active = serializers.IntegerField(required=False, label=_("是否生效"))
    splitable = serializers.CharField(required=False, label=_("是否可以拆分分配"))
    cpu = serializers.FloatField(required=False, label=_("CPU数量，单位core"))
    memory = serializers.FloatField(required=False, label=_("内存数据量，单位MB"))
    gpu = serializers.FloatField(required=False, label=_("GPU数量，单位core类型"))
    disk = serializers.FloatField(required=False, label=_("存储容量，单位MB"))
    net = serializers.FloatField(required=False, label=_("网络带宽，单位bit"))
    slot = serializers.FloatField(required=False, label=_("处理槽位，单位个"))
    connection_info = serializers.CharField(required=False, allow_blank=True, allow_null=True, label=_("集群连接信息"))
    priority = serializers.IntegerField(required=False, allow_null=True, label=_("优先度"))
    belongs_to = serializers.CharField(required=False, allow_blank=True, allow_null=True, label=_("所属父集群"))
    update_type = serializers.CharField(label=_("资源更新类型(full:全量更新，incremental：增量）"))

    def validate_resource_type(self, resource_type):
        resource_type_arrays = ["processing", "storage", "databus", "schedule"]
        if resource_type not in resource_type_arrays:
            raise ValidationError(
                _("非法资源分类: %(resource_type)s，目前仅支持文件类型（%(resource_type_arrays)s）")
                % {"resource_type": resource_type, "resource_type_arrays": ", ".join(resource_type_arrays)}
            )
        return resource_type

    def validate_belongs_to(self, belongs_to):
        if belongs_to:
            parent_cluster = resource_cluster_config.filter_list(cluster_id=belongs_to)
            if not parent_cluster or len(parent_cluster) <= 0:
                raise ValidationError("父集群:%s不存在" % belongs_to)
        return belongs_to

    def validate(self, attrs):
        if "resource_group_id" not in attrs and "cluster_group" not in attrs:
            raise ValidationError(_("resource_group_id 和 cluster_group 必须填一个"))

        return attrs


class UpdateClusterRegisterCapacitySerializer(serializers.Serializer):
    resource_type = serializers.CharField(label=_("资源分类"))
    service_type = serializers.CharField(label=_("服务类型"))
    src_cluster_id = serializers.CharField(label=_("集群ID"))
    cluster_type = serializers.CharField(label=_("集群类型"))
    geog_area_code = serializers.CharField(label=_("区域"))
    cpu = serializers.FloatField(required=False, label=_("CPU数量，单位core"))
    memory = serializers.FloatField(required=False, label=_("内存数据量，单位MB"))
    gpu = serializers.FloatField(required=False, label=_("GPU数量，单位core类型"))
    disk = serializers.FloatField(required=False, label=_("存储容量，单位MB"))
    net = serializers.FloatField(required=False, label=_("网络带宽，单位bit"))
    slot = serializers.FloatField(required=False, label=_("处理槽位，单位个"))


class GetClusterRegisterSerializer(serializers.Serializer):
    resource_type = serializers.CharField(label=_("资源分类"))
    service_type = serializers.CharField(label=_("服务类型"))
    src_cluster_id = serializers.CharField(label=_("集群ID"))
    cluster_type = serializers.CharField(label=_("集群类型"))
    geog_area_code = serializers.CharField(label=_("区域"))


class ProjectAddResourceGroupSerializer(serializers.Serializer):
    resource_group_id = serializers.CharField(label=_("资源组ID"))
    reason = serializers.CharField(label=_("申请原因"))


class JobSubmitRecordSerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("任务ID"))
    resource_group_id = serializers.CharField(label=_("资源组ID"))
    geog_area_code = serializers.CharField(label=_("区域"))
    status = serializers.CharField(required=False, label=_("任务状态"))


class UpdateJobSubmitRecordSerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("任务ID"))
    resource_group_id = serializers.CharField(label=_("资源组ID"))
    geog_area_code = serializers.CharField(label=_("区域"))
    status = serializers.CharField(label=_("任务状态"))


class PartialUpdateJobSubmitRecordSerializer(serializers.Serializer):
    status = serializers.CharField(label=_("任务状态"))


class ListJobSubmitRecordSerializer(serializers.Serializer):
    job_id = serializers.CharField(required=False, label=_("任务ID"))
    resource_group_id = serializers.CharField(required=False, label=_("资源组ID"))
    geog_area_code = serializers.CharField(required=False, label=_("区域"))
    status = serializers.CharField(required=False, label=_("任务状态"))
    limit = serializers.IntegerField(required=False, label=_("结果集记录数限制"))

    def validate(self, attrs):
        if "job_id" not in attrs and "resource_group_id" not in attrs:
            raise ValidationError(_("job_id 和 resource_group_id 必须填一个"))
        if "resource_group_id" in attrs and "geog_area_code" not in attrs:
            raise ValidationError(_("传入 resource_group_id 时必须同时传入 geog_area_code"))
        if "status" not in attrs and "limit" not in attrs:
            raise ValidationError(_("status 和 limit 必须填一个"))
        return attrs


class RegisterJobSubmitInstancesSerializer(serializers.Serializer):
    class JobSubmitInstanceSerializer(serializers.Serializer):
        cluster_id = serializers.CharField(label=_("集群ID"))
        cluster_name = serializers.CharField(allow_null=True, label=_("集群名"))
        resource_type = serializers.CharField(label=_("资源类型"))
        cluster_type = serializers.CharField(label=_("集群类型"))
        inst_id = serializers.CharField(label=_("提交实例ID"))

    submit_id = serializers.IntegerField(required=False, label=_("任务提交ID"))
    job_id = serializers.CharField(required=False, label=_("任务ID"))
    resource_group_id = serializers.CharField(required=False, label=_("资源组ID"))
    geog_area_code = serializers.CharField(required=False, label=_("区域"))
    status = serializers.CharField(required=False, label=_("任务状态"))
    instances = serializers.ListField(child=JobSubmitInstanceSerializer(), label=_("提交实例列表"))

    def validate(self, attrs):
        if "submit_id" not in attrs:
            if "job_id" not in attrs or "resource_group_id" not in attrs or "geog_area_code" not in attrs:
                raise ValidationError(_("创建提交记录必须传入 job_id, resource_group_id, geog_area_code"))
        cluster_id_set = set()
        # 校验一次提交中是否有重复的集群，有的话会造成提交实例表主键冲突
        for instance in attrs["instances"]:
            cluster_id = instance["cluster_id"]
            if cluster_id in cluster_id_set:
                raise ValidationError(_("重复的集群ID：%s") % cluster_id)
            cluster_id_set.add(cluster_id)
        return attrs


class RetrieveJobSubmitInstancesSerializer(serializers.Serializer):
    cluster_name = serializers.CharField(required=False, allow_null=True, label=_("集群名"))
    resource_type = serializers.CharField(required=False, label=_("资源类型"))
    cluster_type = serializers.CharField(required=False, label=_("集群类型"))


class QueryJobSubmitInstancesSerializer(RetrieveJobSubmitInstancesSerializer):
    cluster_id = serializers.CharField(required=False, label=_("集群ID"))
    inst_id = serializers.CharField(required=False, label=_("提交实例ID"))

    def validate(self, attrs):
        params = {"cluster_id", "cluster_name", "resource_type", "cluster_type", "inst_id"}
        attr_set = set(attrs.keys())
        if not (params & attr_set):
            raise ValidationError(
                _("至少传入一个查询参数：cluster_id, cluster_name, resource_type, cluster_type, inst_id") % params
            )
        return attrs


class ClearJobSubmitRecordsSerializer(serializers.Serializer):
    threshold_timestamp = serializers.IntegerField(label=_("清理时间阈值"))

    def validate(self, attrs):
        now_timestamp = datetime.datetime.now()
        threshold_timestamp = datetime.datetime.utcfromtimestamp(int(attrs["threshold_timestamp"]) / 1000)
        min_keep_time_day = int(getattr(settings, "MINIMUM_JOB_SUBMIT_RECORD_KEEP_TIME_DAY"))
        # 设置提交记录最小保留时间保护，避免误删
        if (now_timestamp - threshold_timestamp).days < min_keep_time_day:
            raise ValidationError(_("任务资源提交记录最少保留%s天") % min_keep_time_day)
        return attrs
