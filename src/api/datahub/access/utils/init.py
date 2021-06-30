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


from common.log import logger
from datahub.access.exceptions import CollectorError, CollerctorCode
from datahub.access.handlers import CcHandler
from datahub.access.models import (
    AccessManagerConfig,
    AccessScenarioConfig,
    AccessScenarioStorageChannel,
    DatabusChannelClusterConfig,
)
from datahub.access.settings import ACCESS_SCENARIO_DICT
from datahub.storekit.settings import BKDATA_VERSION_TYPE


def init_scenario_storage_channel():
    channel_ids = DatabusChannelClusterConfig.objects.filter(cluster_role="outer", active=True).values_list(
        "id", flat=True
    )

    if not channel_ids:
        logger.error(u"outer channels list is none")
        raise CollectorError(error_code=CollerctorCode.API_RES_ERR)

    logger.error(u"start init AccessScenarioStorageChannel")

    data_scenarios = ACCESS_SCENARIO_DICT.keys()
    scenario_storage_channels = list()
    for channel_id in channel_ids:
        for data_scenario in data_scenarios:
            scenario_storage_channels.append(
                AccessScenarioStorageChannel(
                    data_scenario=data_scenario,
                    storage_channel_id=channel_id,
                    priority=1,
                    created_by="admin",
                    updated_by="admin",
                    description="",
                )
            )
    # 先删除所有记录
    AccessScenarioStorageChannel.objects.all().delete()

    AccessScenarioStorageChannel.objects.bulk_create(scenario_storage_channels)
    logger.error(u"start init finish")


def init_blueking_biz_id():
    bk_biz_id, maintainer = CcHandler.get_biz_info_by_name(u"蓝鲸")

    names = maintainer.replace(";", ",").split(",")[0] if maintainer else "admin"
    manager_conf = AccessManagerConfig.objects.filter(type="bk")
    if not manager_conf:
        AccessManagerConfig.objects.create(
            names=str(bk_biz_id),
            type="bk",
            active=1,
            created_by="admin",
            updated_by="admin",
            description="",
        )

    job_manager = AccessManagerConfig.objects.filter(type="job")
    if not job_manager:
        AccessManagerConfig.objects.create(
            names=names,
            type="job",
            active=1,
            created_by="admin",
            updated_by="admin",
            description="",
        )
    else:
        AccessManagerConfig.objects.filter(id=job_manager[0].id).update(
            names=names,
            type="job",
            active=1,
            created_by="admin",
            updated_by="admin",
            description="",
        )


def patch_scenario():
    """
    sql 会初始化场景，此处将根据实际版本情况调整场景
    :return:
    """
    access_scenarios = ACCESS_SCENARIO_DICT.keys()
    scenarios = AccessScenarioConfig.objects.all()
    if scenarios:
        for scenario in scenarios:
            # 对于企业版, 不支持的场景直接不展示
            if BKDATA_VERSION_TYPE == "ee":
                if scenario.data_scenario_name not in access_scenarios:
                    scenario.delete()
                else:
                    scenario.active = 1
                    scenario.save()
            else:
                # 其他版本，均置灰
                scenario.active = 1 if scenario.data_scenario_name in access_scenarios else 0
                scenario.save()
