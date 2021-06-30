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
import io
import os
import sys
import types

import fastavro
from bkbase.dataflow.core.exec.pipeline import Pipeline
from bkbase.dataflow.stream.metric.metric_input_process import MetricInputProcess
from bkbase.dataflow.stream.storage.avro_kafka_writer import AvroWriter
from bkbase.dataflow.stream.util.utils import get_input_schema
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, udf
from pyspark.sql.types import ArrayType


class StructuredStreamingPipeline(Pipeline):
    def __init__(self, topology):
        super().__init__(topology)
        self.spark = None
        self.runtime_instance = None
        self.__set_env()

    def _generate_user_runtime_instance(self):
        if self.topology.code:
            runtime_module = self._load_user_module("user_module", self.topology.code)
            runtime_class = getattr(runtime_module, "spark_structured_streaming_code_transform")
            runtime_instance = runtime_class(self.topology.user_args, self.spark)
        else:
            runtime_module = importlib.import_module(self.topology.user_main_module)
            runtime_class = getattr(runtime_module, self.topology.user_main_class)
            runtime_instance = runtime_class(self.topology.user_args, self.spark)
        return runtime_instance

    def _load_user_module(self, module_name, module_content):
        if module_name in sys.modules:
            return sys.modules[module_name]
        runtime_module = sys.modules.setdefault(module_name, types.ModuleType(module_name))
        runtime_module.__file__ = module_name
        runtime_module.__package__ = ""
        code = compile(module_content, module_name, "exec")
        exec(code, runtime_module.__dict__)
        return runtime_module

    def _prepare(self):
        builder = SparkSession.builder
        if isinstance(self.topology.engine_conf, dict) and self.topology.engine_conf:
            for key, value in self.topology.engine_conf.items():
                builder.config(key, value)
        self.spark = builder.appName(self.topology.job_id).getOrCreate()
        self.runtime_instance = self._generate_user_runtime_instance()

    def _source(self):
        source_datas = {}
        for node in self.topology.source_nodes:
            avro_reader = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", node.input.get("info"))
                .option("subscribe", node.input.get("topic"))
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", 1000)
            )
            if self.topology.savepoint.start_position == "from_tail":
                avro_reader.option("startingOffsets", "latest")
            elif self.topology.savepoint.start_position == "from_head":
                avro_reader.option("startingOffsets", "earliest")

            avro_df = avro_reader.option("includeTimestamp", "true").load()
            explode_df = avro_df.select(explode(self.decode_avro_record(node)(avro_df.value)).alias("value"))
            input_df = explode_df.select(*self.select_input_fields(explode_df, node))
            source_datas[node.node_id] = input_df
        return source_datas

    def _sink(self, output_dfs):
        if not isinstance(output_dfs, dict):
            raise Exception("Transform method return must be a dict.")
        for sink_node in self.topology.sink_nodes:
            if sink_node.node_id not in output_dfs:
                raise Exception(
                    "Not found the output for node {}, and sink nodes is {}".format(
                        sink_node.node_id, str(output_dfs.keys)
                    )
                )

            output_model_method = getattr(self.runtime_instance, "set_output_mode", None)

            if output_model_method is None or output_model_method().get(sink_node.node_id) is None:
                output_model = "append"
            else:
                output_model = output_model_method().get(sink_node.node_id)

            if output_model not in ["append", "update", "complete"]:
                raise Exception("Not support output mode %s" % output_model)

            checkpoint_hdfs_path = self.topology.savepoint.hdfs_path + "/" + sink_node.node_id

            ds_writer = (
                output_dfs.get(sink_node.node_id)
                .writeStream.outputMode(output_model)
                .option("checkpointLocation", checkpoint_hdfs_path)
                .foreachBatch(AvroWriter(sink_node, self.topology).for_each_batch_writer)
            )
            ds_writer.start().awaitTermination()

    def _transform(self, inputs):
        transform_method = getattr(self.runtime_instance, "transform")
        return transform_method(inputs)

    def _metrics(self, inputs, outputs):
        if len(outputs) == 0 or len(inputs) == 0:
            raise Exception("input or output can not be null")
        # output cnt metrics
        topo = self.topology
        rt = list(outputs.keys())[0]
        # outputs.get(rt).writeStream \
        #    .outputMode('append') \
        #    .foreachBatch(MetricOutputProcess(topo.run_mode, topo.job_id, topo.metric, rt).foreach_batch_writer) \
        #    .start()
        # rt's input cnt\latency metrics
        input_table = list(inputs.keys())[0]
        inputs.get(input_table).writeStream.outputMode("append").foreachBatch(
            MetricInputProcess(topo.run_mode, topo.job_id, topo.metric, rt).foreach_batch_writer
        ).start()

    def submit(self):
        self._prepare()
        inputs = self._source()
        outputs = self._transform(inputs)
        self._metrics(inputs, outputs)
        self._sink(outputs)

    @staticmethod
    def decode_avro_record(node):
        @udf(returnType=ArrayType(get_input_schema(node)))
        def decode_avro_record_(value):
            if value:
                result = []
                utf8_message = value.decode("utf8")
                raw_file = io.BytesIO(utf8_message.encode("ISO-8859-1"))
                raw_records = fastavro.reader(raw_file)
                for item in raw_records:
                    for entry in item["_value_"]:
                        row = {}
                        for field in node.get_field_names():
                            row[field] = entry[field]
                        result.append(row)
                return result
            else:
                return []

        return decode_avro_record_

    @staticmethod
    def select_input_fields(df, node):
        select = []
        for input_field in node.get_field_names():
            select.append(df.value[input_field].alias(input_field))
        return select

    def __set_env(self):
        if (
            "spark.uc.stream.python.path" in self.topology.engine_conf
            and self.topology.engine_conf["spark.uc.stream.python.path"]
        ):
            pyspark_path = self.topology.engine_conf["spark.uc.stream.python.path"]
            os.environ["PYSPARK_PYTHON"] = pyspark_path
