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
import os

import tensorflow as tf

STRATEGY = None
if "TF_CONFIG" in os.environ:
    tf_config = json.loads(os.environ["TF_CONFIG"])
    if "ps" not in tf_config["cluster"]:
        worker_list = tf_config["cluster"]["worker"]
        new_worker_list = []
        for worker in worker_list:
            new_worker_list.append(worker.replace("2222", "2223"))
        tf_config["cluster"]["worker"] = new_worker_list
        os.environ["TF_CONFIG"] = json.dumps(tf_config)
        STRATEGY = tf.distribute.MultiWorkerMirroredStrategy()
    else:
        worker_list = tf_config["cluster"]["worker"]
        ps_list = tf_config["cluster"]["ps"]
        new_worker_list = []
        new_ps_list = []
        for worker in worker_list:
            new_worker_list.append(worker.replace("2222", "2223"))
        for ps in ps_list:
            new_ps_list.append(ps.replace("2222", "2223"))
        tf_config["cluster"]["worker"] = new_worker_list
        tf_config["cluster"]["ps"] = new_ps_list

        if "chief" in tf_config["cluster"]:
            new_chief_list = []
            chief_list = tf_config["cluster"]["chief"]
            for chief in chief_list:
                new_chief_list.append(chief.replace("2222", "2223"))
            tf_config["cluster"]["chief"] = new_chief_list

        os.environ["TF_CONFIG"] = json.dumps(tf_config)
        # tf.compat.v1.disable_v2_behavior()
        cluster_resolver = tf.distribute.cluster_resolver.TFConfigClusterResolver()
        STRATEGY = None
        if cluster_resolver.task_type == "chief":
            # strategy = tf.compat.v1.distribute.experimental.ParameterServerStrategy(cluster_resolver)
            STRATEGY = tf.distribute.experimental.ParameterServerStrategy(cluster_resolver)
            pass
        elif cluster_resolver.task_type in ("worker", "ps"):
            os.environ["GRPC_FAIL_FAST"] = "use_caller"
            server = tf.distribute.Server(
                cluster_resolver.cluster_spec(),
                job_name=cluster_resolver.task_type,
                task_index=cluster_resolver.task_id,
                protocol="grpc",
            )
            # Blocking the process that starts a server from exiting.
            server.join()
