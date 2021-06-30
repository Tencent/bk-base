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

from common.log import logger


class FunctionManager(models.Manager):
    def func_check(self, func_code):
        """
        @summary: 检查改功能是否开放
        @param func_code: 功能ID
        @return: (True/False, 'message')
        """
        try:
            enabled = self.get(func_code=func_code).enabled
            return (True, int(enabled))
        except Exception as e:
            logger.error("检查改功能是否开放发生异常，错误信息：%s" % e)
            return (False, 0)


SERVICE_MODUELS = ("datahub", "dataflow", "modelflow", "datamart", "datamodeling", "datalab")
SERVICE_MODUELS_CHOICES = [(m, m) for m in SERVICE_MODUELS]


class FunctionController(models.Model):
    """
    功能开启控制器
    """

    func_code = models.CharField("功能代号", max_length=64, unique=True)
    func_name = models.CharField("功能名称", max_length=64)
    enabled = models.BooleanField("功能开关", help_text="控制功能是否对外开放，若选择，则该功能将对外开放", default=False)

    create_time = models.DateTimeField("创建时间", auto_now_add=True)
    func_developer = models.TextField("功能开发者", help_text="设置了开发者后就仅开发者可见，多个用分号隔开", null=True, blank=True)

    service_module = models.CharField(
        "服务模块",
        max_length=64,
        help_text="比如 datahub/dataflow/...",
        choices=SERVICE_MODUELS_CHOICES,
        null=True,
        blank=True,
    )
    forced_rewrite = models.BooleanField("是否强制重写", help_text="如果设置了强制重写，则无法被初始化内容覆盖", default=False)

    objects = FunctionManager()

    def has_developer(self):
        if self.func_developer is None or self.func_developer == "":
            return False

        return True

    def get_developers(self):
        if self.has_developer() and isinstance(self.func_developer, str):
            return self.func_developer.split(";")

        return []

    def __unicode__(self):
        return self.func_name

    class Meta:
        app_label = "app_control"
        verbose_name = "功能控制器"
        verbose_name_plural = "功能控制器"
