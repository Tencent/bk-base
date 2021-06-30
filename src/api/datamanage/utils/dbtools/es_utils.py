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
from elasticsearch import Elasticsearch
from elasticsearch import helpers

from common.log import logger
from common.base_crypt import BaseCrypt

from datamanage.pizza_settings import CRYPT_INSTANCE_KEY, ES_TIMEOUT, ES_MAX_RESULT_WINDOW
from datamanage import exceptions as dm_errors
from datamanage.exceptions import DatamanageErrorCode

try:
    from django.conf import settings

    ES_CONFIG = settings.ES_SETTINGS
except Exception as e:
    logger.warning(f'get es_config error, fail for:{str(e)}')
    ES_CONFIG = {}
_es_cache = {}


def init_es_client(es_name='data_trace', config=None):
    es_config = ES_CONFIG.get(es_name, {})
    if config and config is not None:
        es_config = config
    try:
        if _es_cache.get(es_name, False) and _es_cache[es_name].ping():
            _esclient = _es_cache[es_name]
        else:
            base_crypt = BaseCrypt(CRYPT_INSTANCE_KEY)
            auth_tuple = (es_config.get('user', ''), base_crypt.decrypt(es_config.get('token', '')))
            _esclient = Elasticsearch(
                host=es_config.get('host', ''), port=es_config.get('port', ''), http_auth=auth_tuple
            )
            _es_cache[es_name] = _esclient
        return _esclient
    except Exception as e:
        logger.error(
            'get es client(es_name:{},host:{},port:{},user:{},token:{}) failed, reason:{}'.format(
                es_name,
                es_config.get('host', ''),
                es_config.get('port', ''),
                es_config.get('user', ''),
                es_config.get('token', ''),
                e,
            ),
            result_code=DatamanageErrorCode.GET_ES_CLIENT_ERR,
        )
        return False


class ElasticSearchClient(object):
    timeout = ES_TIMEOUT

    def __init__(self, es_name, host, port, user, token):
        self.es_name = es_name
        try:
            port = int(port)
        except ValueError:
            err_msg = 'convert es port:{} to int error'.format(port)
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
            raise dm_errors.GetEsClienError(message_kv={'es_name': self.es_name, 'es_config': self.es_config})
        return es_client

    def bulk_data(self, bulk_data_list, index_name, type_name):
        """report bulk data list to es

        :param bulk_data_list: event list
        :param index_name: index
        :param type_name: type(type of data in the same index, like a table in a database)
        :return:
        """
        if not isinstance(bulk_data_list, list):
            error_msg = 'bulk_data_list:{} is not a list'.format(bulk_data_list)
            logger.error(error_msg)
            raise dm_errors.EsBulkDataError(message_kv={'error_info': error_msg})

        format_bulk_data_list = []
        for data_dict in bulk_data_list:
            if not ('id' in data_dict and data_dict['id']):
                continue
            format_bulk_data_list.append({"_index": index_name, "_source": data_dict, "_type": type_name})
        try:
            helpers.bulk(self.es, format_bulk_data_list)
        except Exception as e:
            logger.error('report bulk data:{} to elasticsearch error, for:{}'.format(format_bulk_data_list, e))
            raise dm_errors.EsBulkDataError(message_kv={'error_info': e})

    def search_data(self, query_tpl_dict, index_name, from_=0, size=ES_MAX_RESULT_WINDOW):
        """search corresponding content of specific dataset_id from es

        :param query_tpl_dict: search condition
        :type query_tpl_dict: dict
        :param index_name: index
        :param from_: from
        :param size: max num of search ret
        :return: search ret
        """
        try:
            search_dict = self.es.search(
                index=index_name, body=query_tpl_dict, from_=from_, size=size, request_timeout=self.timeout
            )
        except Exception as e:
            logger.error('elastic search data error, for:{}'.format(e))
            raise dm_errors.EsSearchError(message_kv={'error_info': e})
        return search_dict
