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

import json

from .base import BaseTest


class TestStorekit(BaseTest):
    def common_create_table(self):
        return self.common_success()

    def storage_result_tables(self):
        data = [{"cluster_name": "xx", "storage_channel_id": 1}]
        return self.success_response(data)

    def get_physical_table_name(self):
        data = {
            "tspider": "mapleleaf_591.etl_p_s_03_591",
            "hdfs": "/kafka/data/591/etl_p_s_03_591",
            "kafka": "591_etl_p_s_03",
            "es": "591_etl_p_s_03",
            "mysql": "mapleleaf_591.etl_p_s_03_591",
            "tredis": "591_etl_p_s_03",
            "druid": "591_etl_p_s_03",
            "hermes": "591_etl_p_s_03",
            "queue": "591_etl_p_s_03",
            "tsdb": "etl_p_s_03_591.etl_p_s_03_591",
            "tdw": "591_etl_p_s_03.table_name",
        }
        return self.success_response(data)

    def es_create_index(self):
        return self.common_success()

    def tspider_is_db_exist(self):
        return self.common_success()

    def tspider_create_table(self):
        return self.common_success()

    def tspider_create_db(self):
        return self.common_success()

    def mysql_create_table(self):
        return self.common_success()

    def mysql_create_db(self):
        return self.common_success()

    def tsdb_create_db(self):
        return self.common_success()

    def storage_cluster_configs(self):
        data = [
            {
                "cluster_name": "es-test",
                "cluster_type": "es",
                "cluster_domain": "localhost",
                "cluster_group": "es_test",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "xxxx",
                "cluster_type": "hdfs",
                "cluster_domain": "localhost",
                "cluster_group": "default",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "mysql-test",
                "cluster_type": "mysql",
                "cluster_domain": "localhost",
                "cluster_group": "mysql",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "tsdb-test",
                "cluster_type": "tsdb",
                "cluster_domain": "localhost",
                "cluster_group": "tsdb",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "tredis-test",
                "cluster_type": "tredis",
                "cluster_domain": "localhost",
                "cluster_group": "tredis",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "druid-test",
                "cluster_type": "druid",
                "cluster_domain": "localhost",
                "cluster_group": "druid",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "hermes-test",
                "cluster_type": "hermes",
                "cluster_domain": "localhost",
                "cluster_group": "xxx",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "queue-test",
                "cluster_type": "queue",
                "cluster_domain": "xxx",
                "cluster_group": "queue",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "tspider-test",
                "cluster_type": "tspider",
                "cluster_domain": "xxx",
                "cluster_group": "test",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "es2-test",
                "cluster_type": "es",
                "cluster_domain": "localhost",
                "cluster_group": "es2_test",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "es3-test",
                "cluster_type": "es",
                "cluster_domain": "localhost",
                "cluster_group": "es",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "es3-test",
                "cluster_type": "es_type",
                "cluster_domain": "localhost",
                "cluster_group": "es",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "es3-test",
                "cluster_type": "es_test_type",
                "cluster_domain": "localhost",
                "cluster_group": "es",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "mysql2-test",
                "cluster_type": "mysql",
                "cluster_domain": "localhost",
                "cluster_group": "mysql",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
            {
                "cluster_name": "mysql3-test",
                "cluster_type": "mysql",
                "cluster_domain": "localhost",
                "cluster_group": "mysql",
                "connection_info": json.dumps(
                    {
                        "interval": 60000,
                        "servicerpc_port": 1,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_cluster_name": "xxxx",
                        "hdfs_conf_dir": "/xx/xx/xx/xx",
                        "hosts": "master-01,master-02",
                        "hdfs_url": "hdfs://xxxx",
                        "flush_size": 1000000,
                        "log_dir": "/xx/xx",
                        "hdfs_default_params": {"dfs.replication": 2},
                        "topic_dir": "/xx/xx/",
                        "rpc_port": 9000,
                    }
                ),
            },
        ]
        return self.success_response(data)

    def scenarios_common(self):
        data = {
            "formal_name": {
                "queue": "Queue",
                "hdfs": "HDFS",
                "tredis": "TRedis",
                "mysql": "MySQL",
                "tspider": "TSpider",
                "hermes": "Hermes",
                "druid": "Druid",
                "es": "Elasticsearch",
                "tsdb": "TSDB",
            },
            "storage_query": ["mysql", "tspider", "hermes", "es", "druid"],
        }
        return self.success_response(data)

    def delete_rollback(self):
        return self.common_success()

    def tsdb_is_table_exist(self):
        return self.common_success()
