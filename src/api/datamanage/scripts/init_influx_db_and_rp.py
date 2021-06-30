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
import os

from influxdb import InfluxDBClient
from datamanage.pizza_settings import DB_SETTINGS, INFLUX_RETENTION_POLICIES

INFLUXDB_IP_KEY = 'INFLUXDB_BKDATA_IP'
INFLUXDB_PORT_KEY = 'INFLUXDB_PORT'
INFLUXDB_USERNAME_KEY = 'INFLUXDB_BKDATA_USER'
INFLUXDB_PASSWORD_KEY = 'INFLUXDB_BKDATA_PASS'


def main():
    ip_list = get_influxdb_ip_list()
    port = os.environ.get(INFLUXDB_PORT_KEY)
    username = os.environ.get(INFLUXDB_USERNAME_KEY)
    password = os.environ.get(INFLUXDB_PASSWORD_KEY)

    for influxdb_ip in ip_list:
        client = InfluxDBClient(
            host=influxdb_ip,
            port=port,
            username=username,
            password=password,
        )
        init_db_and_rp(client)


def get_influxdb_ip_list():
    index = 0
    ip_list = []

    key = '{}{}'.format(INFLUXDB_IP_KEY, index)
    while key in os.environ:
        ip_list.append(os.environ[key])
        index += 1
        key = '{}{}'.format(INFLUXDB_IP_KEY, index)

    return ip_list


def init_db_and_rp(client):
    databases = [x['name'] for x in client.get_list_database()]

    for influx_config in list(DB_SETTINGS.values()):
        database = influx_config['database']
        if database not in databases:
            client.create_database(database)
            print('Init influxdb %s' % database)

        client.switch_database(database)
        try:
            retention_policies = [x['name'] for x in client.get_list_retention_policies()]
            for retention_policy, config in list(INFLUX_RETENTION_POLICIES.items()):
                if retention_policy not in retention_policies:
                    client.create_retention_policy(**config)
                else:
                    client.alter_retention_policy(**config)
                print('Create or update rp(%s) success' % retention_policy)
        except Exception:
            print('Failed to init influxdb %s and its RPs' % database)


if __name__ == '__main__':
    main()
