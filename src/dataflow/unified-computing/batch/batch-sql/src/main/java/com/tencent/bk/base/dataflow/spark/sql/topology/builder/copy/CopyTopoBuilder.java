/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.copy;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.spark.BatchTimeDelta;
import com.tencent.bk.base.dataflow.spark.BatchTimeStamp;
import com.tencent.bk.base.dataflow.spark.sql.parser.SQLNodeJsonParam4JavaParser;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLJavaEnumerations.BatchSQLType;
import com.tencent.bk.base.dataflow.spark.sql.topology.BatchSQLTopology;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology.AbstractBatchTopologyBuilder;
import com.tencent.bk.base.dataflow.spark.topology.HDFSStorageConstructor;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSCopySinkNode.BatchHDFSCopySinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergCopySinkNode.BatchIcebergCopySinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSCopySourceNode.BatchHDFSCopySourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergCopySourceNode.BatchIcebergCopySourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLHDFSSinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.helper.TopoBuilderScalaHelper$;
import com.tencent.bk.base.dataflow.spark.utils.APIUtil$;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import com.tencent.bk.base.dataflow.spark.utils.HadoopUtil;
import scala.Tuple2;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CopyTopoBuilder extends AbstractBatchTopologyBuilder {

    SQLNodeJsonParam4JavaParser parser;
    //copy中resultTableId不是job_id
    String resultTableId;

    private Map<String, Object> runtimeParams;
    private String batchType;

    /**
     * 创建spark copy拓扑
     * @param parameters copy拓扑参数
     */
    public CopyTopoBuilder(Map<String, Object> parameters) {
        super(parameters);
        this.resultTableId = parameters.get("makeup_rt_id").toString();

        this.runtimeParams = APIUtil$.MODULE$.fetchBKSQLFullParamsAsJava(this.resultTableId);
        if (null != this.runtimeParams.get("batch_type")) {
            this.batchType = this.runtimeParams.get("batch_type").toString();
        }

        if (this.batchType != null && "batch_sql_v2".equals(this.batchType)) {
            this.setV2HadoopClusterInfo();
        } else {
            this.parser = new SQLNodeJsonParam4JavaParser(this.resultTableId);
            Tuple2<String, String> clusterTuple2 = APIUtil$.MODULE$.getRTCluster(this.resultTableId);
            this.storageClusterGroup = clusterTuple2._1();
        }

        this.buildNodes(parameters);
    }

    private void buildNodes(Map<String, Object> parameters) {

        long sourceTimeInHour =
                CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)parameters.get("source_schedule_time"));
        long targetTimeInHour =
                CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)parameters.get("schedule_time"));

        long sourceDtEventTime;
        long targetDtEventTime;
        if (this.batchType != null && "batch_sql_v2".equals(this.batchType)) {
            sourceDtEventTime = getV2DtEventTime(sourceTimeInHour);
            targetDtEventTime = getV2DtEventTime(targetTimeInHour);
        } else {
            if (this.parser != null) {
                Tuple2 minWindow = this.parser.getMinWindowSize();
                ConstantVar.PeriodUnit minWindowUnit = (ConstantVar.PeriodUnit) minWindow._2();
                int minWindowSize = (Integer) minWindow._1();
                sourceDtEventTime = TopoBuilderScalaHelper$.MODULE$.getDtEventTimeInMs(
                        sourceTimeInHour, minWindowSize, minWindowUnit);
                targetDtEventTime = TopoBuilderScalaHelper$.MODULE$.getDtEventTimeInMs(
                        targetTimeInHour, minWindowSize, minWindowUnit);
            } else {
                throw new RuntimeException("Failed to initialize copy topo builder param parsers");
            }
        }

        SourceNode sourceNode = this.createSourceNode(parameters, sourceDtEventTime);
        SinkNode sinkNode = this.createSinkNode(parameters, targetDtEventTime);

        List<Node> sinkParentNodeList = new LinkedList<>();
        sinkParentNodeList.add(sourceNode);
        sinkNode.setParents(sinkParentNodeList);
        this.sinkNodes.put(this.resultTableId, sinkNode);
    }

    private SourceNode createSourceNode(Map<String, Object> parameters, long sourceDtEventTime) {
        HDFSStorageConstructor sourceStorageConstructor = new HDFSStorageConstructor(this.resultTableId);

        Map<String, Object> sourceNodeConfMap = new HashMap<>();
        sourceNodeConfMap.put("id", this.resultTableId);
        sourceNodeConfMap.put("name",  this.resultTableId);
        sourceNodeConfMap.put("start_time", sourceDtEventTime);
        sourceNodeConfMap.put("end_time", sourceDtEventTime + 3600 * 1000L);

        SourceNode sourceNode;

        boolean isIcebergDataSource = sourceStorageConstructor.getDataType().equals("iceberg");
        if (!isIcebergDataSource) {
            //为了兼容已有的hdfs parquet存储
            sourceNode = (new BatchHDFSCopySourceNodeBuilder(sourceNodeConfMap)).build();
        } else {
            sourceNodeConfMap.put("iceberg_table_name", sourceStorageConstructor.getPhysicalName());
            sourceNodeConfMap.put("iceberg_conf", sourceStorageConstructor.getIcebergConfAsJava());
            sourceNode = (new BatchIcebergCopySourceNodeBuilder(sourceNodeConfMap)).build();
        }
        return sourceNode;
    }

    private SinkNode createSinkNode(Map<String, Object> parameters, long targetDtEventTime) {
        Map<String, Object> sinkNodeConfMap = new HashMap<>();
        sinkNodeConfMap.put("id", this.resultTableId);
        sinkNodeConfMap.put("name",  this.resultTableId);
        sinkNodeConfMap.put("dt_event_time", targetDtEventTime);

        sinkNodeConfMap.put("output_mode", "overwrite");
        sinkNodeConfMap.put("schedule_time", this.scheduleTime);
        boolean enableDispatchToStorage = Boolean.parseBoolean(parameters.get("dispatch_to_storage").toString());
        sinkNodeConfMap.put("dispatch_to_storage", enableDispatchToStorage);
        if (this.batchType != null && "batch_sql_v2".equals(this.batchType)) {
            sinkNodeConfMap.put("enable_databus_shipper", this.isV2EnableCallShipper());
        } else {
            if (this.parser != null) {
                Map<String, Object> sinkRawStorages = new HashMap<>();
                sinkRawStorages.put("storages", this.parser.getStoragesMapAsJava());
                Map<String, Object> sinkStorages =
                        PeriodicSQLHDFSSinkNodeBuilder.getStorages(resultTableId, sinkRawStorages);
                boolean enableShipper = PeriodicSQLHDFSSinkNodeBuilder.isDatabusShipperEnable(sinkStorages);
                sinkNodeConfMap.put("enable_databus_shipper", enableShipper);
            } else {
                throw new RuntimeException("Failed to initialize copy topo builder param parsers");
            }
        }


        sinkNodeConfMap.put("enable_reserved_schema", true);
        sinkNodeConfMap.put("force_update_reserved_schema", true);
        SinkNode sinkNode;

        HDFSStorageConstructor sourceStorageConstructor = new HDFSStorageConstructor(this.resultTableId);
        boolean isIcebergDataSource = sourceStorageConstructor.getDataType().equals("iceberg");
        // 当激活发送到正式存储时，旧逻辑会move copy目标路径下的文件到正式的路径下，
        // 但是在iceberg我们无法控制文件具体存储路径，所以当发送到正式存储激活时直接将copy数据存储到正式存储的iceberg中，
        // 如果没有激活则copy到hdfs中的copy路径下
        if (isIcebergDataSource) {
            sinkNodeConfMap.put("data_type", "iceberg");
            sinkNodeConfMap.put("copy_format", "parquet");
            sinkNodeConfMap.put("iceberg_table_name", sourceStorageConstructor.getPhysicalName());
            sinkNodeConfMap.put("iceberg_conf", sourceStorageConstructor.getIcebergConfAsJava());
            sinkNodeConfMap.put("enable_iceberg_partition_column", true);
            sinkNode = (new BatchIcebergCopySinkNodeBuilder(sinkNodeConfMap)).build();
        } else {
            sinkNodeConfMap.put("data_type", "parquet");
            sinkNode = (new BatchHDFSCopySinkNodeBuilder(sinkNodeConfMap)).build();
        }
        return sinkNode;
    }

    private long getV2DtEventTime(long scheduleTime) {
        BatchTimeStamp scheduleTimeObj = new BatchTimeStamp(scheduleTime);
        Map<String, Object> nodeInfo = (Map<String, Object>)this.runtimeParams.get("nodes");
        Map<String, Object> sinkNodesInfo = (Map<String, Object>)nodeInfo.get("sink");
        Map<String, Object> sinkRtInfo = (Map<String, Object>)sinkNodesInfo.get(this.resultTableId);
        BatchTimeDelta dataOffsetObj = new BatchTimeDelta((String)sinkRtInfo.get("data_time_offset"));
        long dtEventTimeStamp = scheduleTimeObj.minus(dataOffsetObj).getTimeInMilliseconds();
        return dtEventTimeStamp;
    }

    private void setV2HadoopClusterInfo() {
        Map<String, Object> nodeInfo = (Map<String, Object>)this.runtimeParams.get("nodes");
        Map<String, Object> sinkNodesInfo = (Map<String, Object>)nodeInfo.get("sink");
        Map<String, Object> info = (Map<String, Object>)sinkNodesInfo.get(this.resultTableId);
        Map<String, Object> storageMap = (Map<String, Object>)info.get("storage_conf");
        this.storageClusterGroup = storageMap.get("cluster_group").toString();
        HadoopUtil.setClusterGroup(this.storageClusterGroup);
        HadoopUtil.setClusterName(storageMap.get("cluster_name").toString());
    }

    private boolean isV2EnableCallShipper() {
        Map<String, Object> nodeInfo = (Map<String, Object>)this.runtimeParams.get("nodes");
        Map<String, Object> sinkNodesInfo = (Map<String, Object>)nodeInfo.get("sink");
        Map<String, Object> sinkRtInfo = (Map<String, Object>)sinkNodesInfo.get(this.resultTableId);
        return (boolean)sinkRtInfo.get("call_databus_shipper");
    }

    @Override
    public BatchSQLTopology build() {
        this.fillNodesFromSinkParents();
        this.buildUDF(null);
        BatchSQLTopology batchSQLTopology = new BatchSQLTopology(this);
        batchSQLTopology.setBatchType(BatchSQLType.DATA_MAKEUP);
        return batchSQLTopology;
    }
}
