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

import importlib
import os
import sys
import zipfile

from bkbase.dataflow.batch.api import batch_api
from bkbase.dataflow.batch.conf import batch_conf
from bkbase.dataflow.batch.exception.batch_exception import BatchAPIHttpException
from bkbase.dataflow.batch.exec import sink_dataframe_handler, source_dataframe_handler
from bkbase.dataflow.batch.gateway import py_gateway
from bkbase.dataflow.batch.monitor.monitor_handler import MonitorHandler
from bkbase.dataflow.batch.utils import batch_logger, batch_utils
from bkbase.dataflow.core.exec.pipeline import Pipeline
from jobnavi.exception import JobNaviNoDataException


class BatchPipeline(Pipeline):
    def __init__(self, topology):
        super().__init__(topology)
        self.spark_conf = batch_conf.get_batch_spark_conf()
        if topology.engine_conf is not None and len(topology.engine_conf) > 0:
            for key in topology.engine_conf:
                self.spark_conf.set(key, topology.engine_conf[key])
                batch_logger.info("Added spark conf {}: {}".format(key, topology.engine_conf[key]))
        self.spark_conf.set(batch_conf.SCHEDULE_TIME_CONF, str(topology.schedule_time))
        self.spark_conf.setAppName(self.topology.job_id)
        self.__set_env()
        # self.__import_user_zip()
        self.spark = py_gateway.launch_spark_session(self.spark_conf)
        batch_logger.info("Current Application ID: {}".format(self.spark.sparkContext.applicationId))
        batch_logger.info("Start to add pyfile {}".format(self.topology.user_pkg_path))
        self.spark.sparkContext.addPyFile(self.topology.user_pkg_path)
        batch_logger.info("Finish adding pyfile {}".format(self.topology.user_pkg_path))
        self.transform_instance = self.__generate_user_transform_instance()
        self.monitors = {}
        if batch_conf.ENABLE_MONITOR:
            self.enable_monitor()

        self.__add_hdfs_conf(self.spark.sparkContext._jsc.hadoopConfiguration())

    def enable_monitor(self):
        for sink_node in self.topology.sink_nodes:
            self.monitors[sink_node] = MonitorHandler(self.topology, sink_node)

    def send_monitor(self):
        if batch_conf.ENABLE_MONITOR:
            try:
                batch_logger.info("Pipeline send monitor")
                for monitor in self.monitors:
                    self.monitors[monitor].send_report()
            except BatchAPIHttpException as e:
                batch_logger.warn(e)
                batch_logger.error(e)
                return
        else:
            batch_logger.info("Monitor disabled, won't send report data")

    def check_and_add_monitor_input(self, paths, rt_id):
        if len(self.monitors) != 0:
            c = batch_utils.count_parquet_row_num(self.spark, paths)
            for node_monitor in self.monitors:
                self.monitors[node_monitor].add_input_count(c, rt_id)
            batch_logger.info("Add input count {} for {}".format(c, rt_id))

    def check_and_add_monitor_output(self, paths, rt_id):
        if len(self.monitors) != 0:
            c = batch_utils.count_parquet_row_num(self.spark, paths)
            self.monitors[rt_id].add_output_count(c, rt_id)
            batch_logger.info("Add output count {} for {}".format(c, rt_id))

    def source(self):
        batch_logger.info("Pipeline source start")
        return self.__create_source_dataframes(self.topology.source_nodes)

    def transform(self, dataframes_dict):
        batch_logger.info("Pipeline transform start")
        transform_method = getattr(self.transform_instance, "transform")
        result_dict = transform_method(dataframes_dict)
        return result_dict

    def sink(self, dataframes_dict):
        batch_logger.info("Pipeline sink start")
        self.__sink_dataframes(dataframes_dict, self.topology.sink_nodes)

    def submit(self):
        try:
            source_df_dict = self.source()
            batch_logger.info("source_dict: {}".format(source_df_dict))
            transform_df_dict = self.transform(source_df_dict)
            batch_logger.info("transform_dict: {}".format(transform_df_dict))
            self.sink(transform_df_dict)
            self.send_monitor()
            batch_logger.info("pipeline submit finished")
        except Exception as e:
            self.send_monitor()
            raise e

    def __generate_user_transform_instance(self):
        transform_module = importlib.import_module(self.topology.user_main_module)
        transform_class = getattr(transform_module, self.topology.user_main_class)
        transform_instance = transform_class(self.topology.user_args, self.spark)
        return transform_instance

    def __create_source_dataframes(self, source_nodes):
        dataframe_dict = {}
        is_input_empty = True
        for node in source_nodes:
            dataframe_dict[node] = self.__create_source_dataframe(source_nodes[node])

            if len(dataframe_dict[node].take(1)) != 0:
                is_input_empty = False

        if is_input_empty:
            batch_logger.warn("No input data is found")
            raise JobNaviNoDataException("输入没有数据")

        return dataframe_dict

    def __create_source_dataframe(self, source_node):
        if source_node.self_dependency_mode == "upstream" or source_node.self_dependency_mode == "current":
            df = source_dataframe_handler.create_hdfs_self_dependency_src_df(source_node, self)
        else:
            df = source_dataframe_handler.create_hdfs_src_df(source_node, self)
        return df

    def __sink_dataframes(self, dataframe_dict, sink_nodes):
        save_mode = None
        save_mode_methold = getattr(self.transform_instance, "set_output_mode", None)
        if save_mode_methold is not None:
            save_mode = save_mode_methold()
            batch_logger.info("User defined save mode: {}".format(save_mode))

        is_output_empty = True

        for sink_node in sink_nodes:
            if sink_node in dataframe_dict:
                save_type = save_mode[sink_node] if save_mode is not None and sink_node in save_mode else "overwrite"
                self.__sink_dataframe(dataframe_dict[sink_node], sink_nodes[sink_node], save_type)
            else:
                raise Exception("Can't find required dataframe for node_id {}".format(sink_node))

            row_count = batch_utils.count_parquet_row_num(self.spark, [sink_nodes[sink_node].output])
            batch_logger.info("Output count for {}: {}".format(sink_node, row_count))

            if row_count > 0:
                is_output_empty = False

                # call storekit maintain
                day_diff = batch_utils.get_days_diff(
                    int(sink_nodes[sink_node].event_time), int(batch_utils.get_current_timestamp())
                )
                try:
                    batch_api.call_storekit_maintain(sink_node, "-{}".format(day_diff))
                except BatchAPIHttpException as e:
                    batch_logger.warn(e)
                    batch_logger.error(e)

            # call databus shipper
            if sink_nodes[sink_node].extra_storage:
                try:
                    batch_logger.info("Extra storage enabled, Try to call storage shipper")
                    batch_api.add_connector(sink_node, sink_nodes[sink_node].output, self.topology.schedule_time)
                except BatchAPIHttpException as e:
                    batch_logger.warn(e)
                    batch_logger.error(e)

        if is_output_empty:
            raise JobNaviNoDataException("输出没有数据，请检查计算逻辑")

    def __sink_dataframe(self, df, sink_node, mode):
        sink_dataframe_handler.sink_dataframe(df, sink_node, self, mode)

    def __add_hdfs_conf(self, hadoop_conf):
        hadoop_conf.addResource("core-site.xml")
        hadoop_conf.addResource("hdfs-site.xml")

        for sink_node in self.topology.sink_nodes:
            cluster_group = str(self.topology.sink_nodes[sink_node].storages["cluster_group"])
            cluster_core_file = "core-site.xml.{}".format(cluster_group)
            cluster_hdfs_file = "hdfs-site.xml.{}".format(cluster_group)
            hadoop_conf.addResource(cluster_core_file)
            hadoop_conf.addResource(cluster_hdfs_file)
            batch_logger.info("Added hadoop conf file: {}".format(cluster_core_file))
            batch_logger.info("Added hadoop conf file: {}".format(cluster_hdfs_file))

    def __unzip_zip_file(self, zipfile_path, target_dir):
        batch_logger.info("Start to unzip {} to {}".format(zipfile_path, target_dir))
        with zipfile.ZipFile(zipfile_path, "r") as zip_ref:
            zip_ref.extractall(target_dir)
        batch_logger.info("Unzip user code completed")

    def __download_user_pkg(self):
        target_dir = batch_utils.create_java_tmp_dir()
        batch_logger.info("Create temporary directory for user zip file: {}".format(target_dir))
        source_path = self.topology.user_pkg_path
        filename = os.path.basename(source_path)
        batch_logger.info("Start to download user package from {} to {}".format(source_path, target_dir))
        batch_utils.download_file(source_path, target_dir, filename, self.spark_conf)
        batch_logger.info("Download completed")
        return os.path.join(target_dir, filename)

    def __import_user_zip(self):
        user_zip = self.__download_user_pkg()
        sys.path.insert(0, user_zip)
        self.spark_conf.set("spark.submit.pyFiles", user_zip)

    def __set_env(self):
        pyspark_path = self.spark_conf.get("spark.uc.batch.python.path")
        batch_logger.info("Set up pyspark python path for executor: {}".format(pyspark_path))
        os.environ["PYSPARK_PYTHON"] = pyspark_path
