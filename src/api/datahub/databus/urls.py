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
from django.conf.urls import include, url
from rest_framework.routers import DefaultRouter

from . import (
    admin_views,
    channel_views,
    clean_views,
    cluster_views,
    data_storage_views,
    data_storages_advanced_views,
    datanode_views,
    event_views,
    import_hdfs_views,
    init_databus,
    migration_views,
    queue_auth_views,
    queue_user_views,
    rawdata_views,
    rt_views,
    scenarios_views,
    task_views,
)

router = DefaultRouter(trailing_slash=True)

# Add the generated REST URLs
router.register(r"init_databus", init_databus.InitDatabusViewset, basename="init_databus")
router.register(r"channels", channel_views.ChannelViewset, basename="channels")
router.register(r"cleans", clean_views.CleanViewset, basename="cleans")
router.register(r"clusters", cluster_views.ClusterViewset, basename="clusters")
router.register(
    r"clusters/(?P<cluster_name>[^/.]+)/connectors",
    cluster_views.ConnectorViewset,
    basename="cluster_connector",
)
router.register(r"events", event_views.EventViewset, basename="events")
router.register(r"queue_auths", queue_auth_views.QueueAuthViewset, basename="queue_auths")
router.register(r"queue_users", queue_user_views.QueueUserViewset, basename="queue_users")
router.register(r"rawdatas", rawdata_views.RawdataViewset, basename="rawdatas")
router.register(r"result_tables", rt_views.RtViewset, basename="result_tables")
router.register(r"import_hdfs", import_hdfs_views.ImportHdfsViewSet, basename="import_hdfs")
router.register(r"datanodes", datanode_views.DataNodeViewSet, basename="datanodes")
router.register(r"tasks/puller", task_views.PullerViewset, basename="puller")
router.register(r"tasks/transport", task_views.TransportViewSet, basename="transport")
router.register(r"tasks", task_views.TaskViewset, basename="tasks")
router.register(r"migrations", migration_views.MigrationViewset, basename="migrations")
router.register(r"scenarios", scenarios_views.ScenariosViewSet, basename="scenarios")
router.register(r"data_storages", data_storage_views.DataStorageViewset, basename="data_storage")
router.register(
    r"data_storages_advanced/(?P<result_table_id>[^/.]+)",
    data_storages_advanced_views.DataStoragesAdvancedViewset,
    basename="data_storages_advanced",
)
router.register(r"admin", admin_views.AdminViewSet, basename="admin")


try:
    from datahub.databus.extend import urls as extend_urls

    extend_urlpatterns = extend_urls.urlpatterns
except ImportError as e:
    extend_urlpatterns = []
    logger.info(u"failed to import extend urls, %s" % (str(e)))

urlpatterns = extend_urlpatterns + [url(r"^", include(router.urls))]
