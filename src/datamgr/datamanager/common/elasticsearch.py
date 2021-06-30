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
from __future__ import absolute_import, print_function, unicode_literals

import logging

from elasticsearch import Elasticsearch, helpers

from common.base_crypt import BaseCrypt
from common.exceptions import EsBulkDataError, GetEsClienError
from conf.settings import CRYPT_INSTANCE_KEY, ES_SETTINGS, ES_TIMEOUT

# 用于存放es client
_es_cache = {}

logger = logging.getLogger(__name__)


def init_es_client(es_name="data_trace", config=None):
    es_config = ES_SETTINGS.get(es_name, {})
    if config and config is not None:
        es_config = config
    try:
        if _es_cache.get(es_name, False) and _es_cache[es_name].ping():
            _esclient = _es_cache[es_name]
        else:
            base_crypt = BaseCrypt(CRYPT_INSTANCE_KEY)
            auth_tuple = (
                es_config.get("user", ""),
                (base_crypt.decrypt(es_config.get("token", ""))).decode("utf-8"),
            )
            _esclient = Elasticsearch(
                host=es_config.get("host", ""),
                port=es_config.get("port", ""),
                http_auth=auth_tuple,
            )
            _es_cache[es_name] = _esclient
        return _esclient
    except Exception as e:
        logger.error(
            "get es client(es_name:{},host:{},port:{},user:{},token:{}) failed, reason:{}".format(
                es_name,
                es_config.get("host", ""),
                es_config.get("port", ""),
                es_config.get("user", ""),
                es_config.get("token", ""),
                e,
            )
        )
        return False


class ElasticSearchClient(object):
    timeout = ES_TIMEOUT

    def __init__(self, es_name, host, port, user, token):
        self.es_name = es_name
        try:
            port = int(port)
        except ValueError:
            err_msg = "convert es port:{} to int error".format(port)
            logger.error(err_msg)
            raise ValueError(err_msg)
        self.es_config = dict(host=host, port=port, user=user, token=token)
        self.es = self.get_es_client()

    def get_es_client(self):
        """get es client by es config

        :return: es client
        """
        es_client = init_es_client(self.es_name, self.es_config)
        if not es_client:
            raise GetEsClienError(
                message_kv={"es_name": self.es_name, "es_config": self.es_config}
            )
        return es_client

    def bulk_data(self, bulk_data_list, index_name, type_name):
        """report bulk data list to es

        :param bulk_data_list: event list
        :param index_name: index
        :param type_name: type(type of data in the same index, like a table in a database)
        :return:
        """
        if not isinstance(bulk_data_list, list):
            error_msg = "bulk_data_list:{} is not a list".format(bulk_data_list)
            logger.error(error_msg)
            raise EsBulkDataError(message_kv={"error_info": error_msg})

        format_bulk_data_list = []
        for data_dict in bulk_data_list:
            if not ("id" in data_dict and data_dict["id"]):
                continue
            format_bulk_data_list.append(
                {"_index": index_name, "_source": data_dict, "_type": type_name}
            )
        try:
            helpers.bulk(self.es, format_bulk_data_list)
        except Exception as e:
            logger.error(
                "report bulk data:{} to elasticsearch error, for:{}".format(
                    format_bulk_data_list, e
                )
            )
            raise EsBulkDataError(message_kv={"error_info": e})
