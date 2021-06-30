# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from django.db import models
from django.forms import model_to_dict
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _

from apps.models import JsonField
from apps.utils.time_handler import strftime_local


class ResultTableSelectedRecord(models.Model):
    project_id = models.IntegerField(_("项目id(待废弃)"), null=True)
    result_table_id = models.TextField(_("结果表"))
    operator = models.CharField(_("查询人"), max_length=128)
    time = models.DateTimeField(_("查询时间"), auto_now=True)

    @classmethod
    def set_selected_history(cls, **kwargs):
        cls.objects.update_or_create(defaults={"time": timezone.now()}, **kwargs)

    @classmethod
    def delete_selected_history(cls, **kwargs):
        cls.objects.filter(**kwargs).delete()

    @classmethod
    def list_selected_history(cls, operator):
        """
        获取用户最近选择的10个结果表
        :param operator:
        :return:
        """
        # 获取个人在项目中最近十条选择结果表历史
        rt_ids = cls.objects.filter(operator=operator).order_by("-time")[:10].values_list("result_table_id", flat=True)
        return list(rt_ids)

    class Meta:
        verbose_name = "【查询记录】结果表选择历史"
        verbose_name_plural = "【查询记录】结果表选择历史"


class ResultTableQueryRecord(models.Model):
    project_id = models.IntegerField(_("项目id"), null=True)
    result_table_ids = models.TextField(_("结果表"))
    operator = models.CharField(_("查询人"), max_length=128)
    time = models.DateTimeField(_("查询时间"), auto_now=True)
    sql = models.TextField(_("查询SQL"), null=True)
    storage_type = models.CharField(_("存储类型"), max_length=128, null=True, blank=True)
    search_range_start_time = models.DateTimeField(_("ES查询开始时间"), null=True)
    search_range_end_time = models.DateTimeField(_("ES查询结束时间"), null=True)

    keyword = models.TextField(verbose_name=_("搜索关键字"), null=True)
    time_taken = models.CharField(_("耗时"), max_length=32, default=0)
    total = models.IntegerField(verbose_name=_("返回条数"), default=0)

    result = models.BooleanField(verbose_name=_("查询结果"), default=True)
    err_msg = models.TextField(verbose_name=_("错误信息"), null=True)

    class Meta:
        verbose_name = _("【查询记录】DB查询")
        verbose_name_plural = _("【查询记录】DB查询")

    @classmethod
    def list_query_history(cls, operator, rt_id, storage_type):
        records = cls.objects.filter(operator=operator, result_table_ids=rt_id, storage_type=storage_type).order_by(
            "-time"
        )

        return_result = []
        for _r in records:
            if _r.keyword or _r.sql:
                history_dict = model_to_dict(_r)
                history_dict.update(
                    {
                        "time": strftime_local(_r.time),
                        "search_range_start_time": strftime_local(_r.search_range_start_time),
                        "search_range_end_time": strftime_local(_r.search_range_end_time),
                    }
                )
                return_result.append(history_dict)
        return return_result


class UserOperateRecord(models.Model):
    """
    用户操作流水
    """

    # @TODO 是否有必要保留
    OPERATE_TYPES = (("ProjectViewSet.create", "创建项目"),)
    request_id = models.CharField("请求ID", db_index=True, max_length=64)
    operator = models.CharField("操作者", db_index=True, max_length=32)
    operate_time = models.DateTimeField("操作时间", auto_now=True)
    operate_type = models.CharField("操作类型", db_index=True, choices=OPERATE_TYPES, max_length=64)
    url = models.CharField("接口url", max_length=256, null=True, blank=True)
    method = models.CharField("请求方法", max_length=1024, null=True, blank=True)
    operate_params = models.TextField("操作参数", null=True, blank=True)
    operate_result = models.TextField("操作返回", null=True, blank=True)
    operate_final = models.CharField("操作结果", max_length=64, null=True, blank=True)
    project_id = models.IntegerField("项目ID", db_index=True, null=True, blank=True)
    operate_object = models.CharField("操作对象", db_index=True, max_length=64, null=True, blank=True)
    operate_object_id = models.CharField("操作对象id", max_length=64, null=True, blank=True)

    extra_data = models.TextField("其它", null=True, blank=True)

    def __unicode__(self):
        return "{}-{}-{}".format(self.operator, self.get_operate_type_display(), self.url)

    class Meta:
        verbose_name = "【用户操作】流水日志"
        verbose_name_plural = "【用户操作】流水日志"


class QQRTXMapping(models.Model):
    rtx = models.CharField("RTX", max_length=64)
    qq = models.CharField("QQ", max_length=64)

    def __unicode__(self):
        return "{},{}".format(self.rtx, self.qq)

    class Meta:
        verbose_name = "【配置信息】QQ-RTX映射"
        verbose_name_plural = "【配置信息】QQ-RTX映射"


class OAuthApplication(models.Model):
    """
    支持 OAuth 授权的应用
    """

    bk_app_code = models.CharField("应用ID", db_index=True, max_length=256)
    bk_app_name = models.CharField("应用名称", max_length=256)
    bk_app_logo = models.CharField("应用LOGO", max_length=512)
    managers = models.CharField("应用负责人", max_length=256)

    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_at = models.DateTimeField("更新时间", auto_now=True)

    class Meta:
        verbose_name = "【OAuth】第三方授权应用"
        verbose_name_plural = "【OAuth】第三方授权应用"


class DataMartSearchRecord(models.Model):
    tag_ids = JsonField(_("标签"))
    me_type = models.CharField(_("数据分类/标准分类"), max_length=128)
    tag_code = models.CharField(_("数据地图分类"), max_length=128)
    created_by = models.CharField(_("创建者"), null=True, max_length=128)
    storage_type = models.CharField(_("存储类型"), null=True, max_length=128)
    created_at_start = models.DateTimeField(_("起始创建时间"), null=True)
    created_at_end = models.DateTimeField(_("终止创建时间"), null=True)
    heat_operate = models.CharField(_("热度比较运算符"), null=True, max_length=128)
    heat_score = models.FloatField(_("热度评分"), null=True, max_length=128)
    range_operate = models.CharField(_("广度比较运算符"), null=True, max_length=128)
    range_score = models.FloatField(_("广度评分"), null=True, max_length=128)
    importance_operate = models.CharField(_("重要度比较运算符"), null=True, max_length=128)
    importance_score = models.FloatField(_("重要度评分"), null=True, max_length=128)
    asset_value_operate = models.CharField(_("价值比较运算符"), null=True, max_length=128)
    asset_value_score = models.FloatField(_("价值评分"), null=True, max_length=128)
    assetvalue_to_cost_operate = models.CharField(_("收益比比较运算符"), null=True, max_length=128)
    assetvalue_to_cost = models.FloatField(_("收益比"), null=True, max_length=128)
    storage_capacity_operate = models.CharField(_("存储成本比较运算符"), null=True, max_length=128)
    storage_capacity = models.FloatField(_("存储成本"), null=True, max_length=128)
    bk_biz_id = models.IntegerField(_("业务id"), null=True)
    project_id = models.IntegerField(_("项目id"), null=True)
    data_set_type = models.CharField(_("数据集类型"), max_length=128)
    platform = models.CharField(_("数据所在系统"), max_length=128)
    keyword = models.TextField(_("搜索关键字"), null=True, max_length=128)
    cal_type = JsonField(_("是否标准化"))
    order_time = models.CharField(_("按时间排序"), null=True, max_length=128)
    order_heat = models.CharField(_("按热度评分排序"), null=True, max_length=128)
    order_range = models.CharField(_("按广度评分排序"), null=True, max_length=128)
    order_asset_value = models.CharField(_("按价值评分排序"), null=True, max_length=128)
    order_importance = models.CharField(_("按重要度评分排序"), null=True, max_length=128)
    order_storage_capacity = models.CharField(_("按存储大小排序"), null=True, max_length=128)
    order_assetvalue_to_cost = models.CharField(_("按收益比排序"), null=True, max_length=128)
    is_standard = models.BooleanField(_("是否标准化数据"), default=False)
    operator = models.CharField(_("查询人"), db_index=True, max_length=128)
    standard_content_id = models.IntegerField(_("标准内容id"), null=True)
    time = models.DateTimeField(_("查询时间"), db_index=True, auto_now=True)

    class Meta:
        verbose_name = "数据集市查询历史记录"
        verbose_name_plural = "数据集市查询历史记录"
