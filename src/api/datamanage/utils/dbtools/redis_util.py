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


import redis

from common.log import logger

from datamanage.exceptions import DatamanageErrorCode

try:
    from django.conf import settings

    REDIS_CONFIG = settings.REDIS_CONFIG
except Exception as e:
    logger.warning(f'{str(e)}')
    REDIS_CONFIG = {}
_redis_cache = {}


def init_redis_conn(db='default', config={}):
    redis_config = REDIS_CONFIG.get(db, {})
    redis_cluster = redis_config.get('cluster', False)
    if config and config != {}:
        redis_config = config
    try:
        if _redis_cache.get(db, False) and _redis_cache[db].ping():
            _redis = _redis_cache[db]
        else:
            logger.info('connect redis[%s]' % db)
            if redis_cluster:
                _redis = redis.StrictRedisCluster(
                    host=redis_config.get('host', ''),
                    port=redis_config.get('port', 6379),
                    password=redis_config.get('password', ''),
                    socket_timeout=3,
                )
            else:
                _redis = redis.StrictRedis(
                    host=redis_config.get('host', ''),
                    port=redis_config.get('port', 6379),
                    password=redis_config.get('password', ''),
                    socket_timeout=3,
                )
            _redis_cache[db] = _redis
        return _redis
    except Exception as e:
        logger.error('connect redis(%s) failed, reason %s' % (db, e), DatamanageErrorCode.REDIS_CONN_ERR)
        return False


def redis_get(key, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.get(key)
    except Exception as e:
        logger.error('query redis(%s) failed, reason %s, key %s' % (db, e, key), DatamanageErrorCode.REDIS_QUERY_ERR)
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_get(key, db, config, retry - 1)
        return False


def redis_set(key, value, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.set(key, value)
    except Exception as e:
        logger.error(
            'set redis(%s) failed, reason %s, key %s, value %s' % (db, e, key, value),
            DatamanageErrorCode.REDIS_SAVE_ERR,
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_set(key, value, db, config, retry - 1)
        return False


def redis_hkeys(name, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.hkeys(name)
    except Exception as e:
        logger.error(
            'query redis(%s) HASH KEY failed, reason %s, [hash] name  %s' % (db, e, name),
            DatamanageErrorCode.REDIS_QUERY_ERR,
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_hkeys(name, db, config, retry - 1)
        return False


def redis_hget(name, key, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.hget(name, key)
    except Exception as e:
        logger.error(
            'query redis(%s) failed, reason %s, [hash] name  %s key %s' % (db, e, name, key),
            DatamanageErrorCode.REDIS_QUERY_ERR,
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_hget(name, key, db, config, retry - 1)
        return False


def redis_hset(name, key, value, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.hset(name, key, value)
    except Exception as e:
        logger.error(
            'set redis(%s) failed, reason %s, [hash] name  %s key %s, value %s' % (db, e, name, key, value),
            DatamanageErrorCode.REDIS_SAVE_ERR,
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_hset(name, key, value, db, config, retry - 1)
        return False


def redis_hdel(name, keys, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.hdel(name, *keys)
    except Exception as e:
        logger.error(
            'delete redis(%s) hashkey  failed, reason %s, [hash] name  %s key %s' % (db, e, name, keys),
            DatamanageErrorCode.REDIS_SAVE_ERR,
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_hdel(name, keys, db, config, retry - 1)
        return False


def redis_delete(name, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        if type(name) is not list:
            del_list = [name]
        else:
            del_list = name
        return _redis.delete(*del_list)
    except Exception as e:
        logger.error(
            ' delete redis(%s) hashkey failed, reason %s, [hash] name  %s' % (db, e, name),
            DatamanageErrorCode.REDIS_SAVE_ERR,
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_delete(name, db, config, retry - 1)
        return False


def redis_rpop(key, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.rpop(key)
    except Exception as e:
        logger.error(
            'read redis(%s) queue failed, reason %s, key %s' % (db, e, key), DatamanageErrorCode.REDIS_QUERY_ERR
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_rpop(key, db, config, retry - 1)
        return False


def redis_lpush(key, value, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.lpush(key, value)
    except Exception as e:
        logger.error(
            'write redis(%s) queue failed, reason %s, key %s value %s' % (db, e, key, value),
            DatamanageErrorCode.REDIS_SAVE_ERR,
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_lpush(key, value, db, config, retry - 1)
        return False


def redis_llen(key, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.llen(key)
    except Exception as e:
        logger.error(
            ' query redis(%s) llen failed, reason %s, [list] key  %s' % (db, e, key),
            DatamanageErrorCode.REDIS_QUERY_ERR,
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_llen(key, db, config, retry - 1)
        return False


def redis_ltrim(key, start, end, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        return _redis.ltrim(key, start, end)
    except Exception as e:
        logger.error(
            ' query redis(%s) ltrim failed, reason %s, [list] key  %s' % (db, e, key),
            DatamanageErrorCode.REDIS_SAVE_ERR,
        )
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_ltrim(key, start, end, db, config, retry - 1)
        return False


def redis_mget(key_list, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        result_list = []
        while len(key_list) > 500:
            result_list.extend(_redis.mget(key_list[:500]))
            key_list = key_list[500:]
        result_list.extend(_redis.mget(key_list))
        return result_list
    except Exception as e:
        logger.error('batch query redis failed, reason : %s' % str(e), DatamanageErrorCode.REDIS_QUERY_ERR)
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_mget(key_list, db, config, retry - 1)
        return False


def redis_mset(data, db='default', config={}, retry=-1):
    try:
        _redis = init_redis_conn(db, config)
        redis_key_list = list(data.keys())
        while len(redis_key_list) > 500:
            part_data = {}
            for key in redis_key_list[:500]:
                part_data[key] = data[key]
                _redis.mset(part_data)
            redis_key_list = redis_key_list[500:]
        part_data = {}
        for key in redis_key_list:
            part_data[key] = data[key]
            _redis.mset(part_data)
        return True
    except Exception as e:
        logger.error('batch query redis failed, reason : %s' % str(e), DatamanageErrorCode.REDIS_SAVE_ERR)
        if retry == -1:
            retry = REDIS_CONFIG.get('retry_cnt', 3)
        if retry > 0:
            return redis_mset(data, db, config, retry - 1)
        return False
