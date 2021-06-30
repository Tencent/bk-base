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


class _ClusterConfig(object):

    def __init__(self, cluster_name, cluster_domain, version, geog_area_code, created_by, created_at, description):
        self.cluster_name = cluster_name
        self.cluster_domain = cluster_domain
        self.version = version
        self.geog_area_code = geog_area_code
        self.created_by = created_by
        self.created_at = created_at
        self.description = description


class _QuerySet(object):

    def __init__(self, cluster_config_list):
        self._cluster_config_list = cluster_config_list

    def filter(self, cluster_name, geog_area_code):
        filter_list = []
        for config in self._cluster_config_list:
            if isinstance(config, _ClusterConfig):
                if config.cluster_name == cluster_name and config.geog_area_code == geog_area_code:
                    filter_list.append(config)
        return _QuerySet(filter_list)

    def all(self):
        return self._cluster_config_list

    def exists(self):
        return len(self._cluster_config_list) > 0

    def save(self, cluster_config):
        self._cluster_config_list.append(cluster_config)


class DataflowJobNaviClusterConfigMocker(object):

    objects = _QuerySet([])

    @staticmethod
    def save(**kwargs):
        cluster_config = _ClusterConfig(kwargs["cluster_name"],
                                        kwargs["cluster_domain"],
                                        kwargs["version"],
                                        kwargs["geog_area_code"],
                                        kwargs["created_by"],
                                        kwargs["created_at"] if "created_at" in kwargs else None,
                                        kwargs["description"] if "description" in kwargs else None)
        DataflowJobNaviClusterConfigMocker.objects.save(cluster_config)

    @staticmethod
    def filter(**kwargs):
        DataflowJobNaviClusterConfigMocker.objects.filter(**kwargs)

    @staticmethod
    def update(cluster_name, geog_area_code, **kwargs):
        pass

    @staticmethod
    def delete(**kwargs):
        pass


def save(tag, **kwargs):
    DataflowJobNaviClusterConfigMocker().save(**kwargs)
