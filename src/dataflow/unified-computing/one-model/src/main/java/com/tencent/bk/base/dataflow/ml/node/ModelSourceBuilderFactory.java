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

package com.tencent.bk.base.dataflow.ml.node;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataNodeType;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.HdfsTableFormat;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.TableType;
import com.tencent.bk.base.dataflow.core.topo.Node.AbstractBuilder;
import com.tencent.bk.base.dataflow.spark.UCSparkConf;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLHDFSSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLIcebergSourceBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug.SQLDebugHDFSSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug.SQLDebugIcebergNodeBuilder;
import java.util.HashMap;
import java.util.Map;

public class ModelSourceBuilderFactory extends AbstractModelBuilderFactory {

    private int debugLabel;

    public ModelSourceBuilderFactory(long scheduleTime, String jobType, boolean isDebug,
            String apiUrl) {
        super(jobType, scheduleTime);
        this.debugLabel = isDebug ? 1 : 0;
        UCSparkConf.sparkConf().set("spark.bkdata.metaapi.url", apiUrl);
        UCSparkConf.sparkConf().set("spark.bkdata.dataflow.url", apiUrl);
        UCSparkConf.sparkConf().set("spark.bkdata.storekit.url", apiUrl);
    }

    @Override
    public AbstractBuilder getBuilder(Map<String, Object> info) {
        this.nodeInfo = info;
        // 获取基本属性
        Map<String, Object> inputInfo = (Map<String, Object>) this.nodeInfo.get("input");
        ConstantVar.DataNodeType sourceNodeType = ConstantVar.DataNodeType
                .valueOf(this.nodeInfo.get("type").toString());
        ConstantVar.TableType tableType = ConstantVar.TableType.valueOf(inputInfo
                .getOrDefault("table_type", TableType.other.toString()).toString());
        ConstantVar.HdfsTableFormat hdfsTableFormat = getHdfsTableFormat(inputInfo.get("format").toString());
        //获取配置
        Map<String, Object> sourceNodeConfMap = this.getConfigMap(tableType, hdfsTableFormat, sourceNodeType);
        //生成builder
        switch (sourceNodeType) {
            case data:
                switch (tableType) {
                    case query_set:
                        switch (hdfsTableFormat) {
                            case iceberg:
                                return new ModelIceBergQuerySetSourceNode.ModelIceBergQuerySetSourceNodeBuilder(
                                        sourceNodeConfMap);
                            case parquet:
                                return new ModelDefaultSourceNode.ModelDefaultSourceNodeBuilder(sourceNodeConfMap);
                            default:
                                throw new RuntimeException("Not supported hdfs table format:" + hdfsTableFormat);
                        }
                    case result_table:
                        switch (hdfsTableFormat) {
                            case iceberg:
                                switch (debugLabel) {
                                    case 1:
                                        return new SQLDebugIcebergNodeBuilder(sourceNodeConfMap);
                                    case 0:
                                        return new PeriodicSQLIcebergSourceBuilder(sourceNodeConfMap);
                                    default:
                                        throw new RuntimeException("Unknown debug label:" + debugLabel);
                                }
                            case parquet:
                                switch (debugLabel) {
                                    case 1:
                                        return new SQLDebugHDFSSourceNodeBuilder(sourceNodeConfMap);
                                    case 0:
                                        return new PeriodicSQLHDFSSourceNodeBuilder(sourceNodeConfMap);
                                    default:
                                        throw new RuntimeException("Unknown debug label:" + debugLabel);
                                }
                            default:
                                throw new RuntimeException("Not supported hdfs table format:" + hdfsTableFormat);
                        }
                    default:
                        throw new RuntimeException("Not supported table type:" + tableType);
                }
            case model:
                sourceNodeConfMap.put("type", sourceNodeType.toString());
                return new ModelDefaultSourceNode.ModelDefaultSourceNodeBuilder(sourceNodeConfMap);
            default:
                throw new RuntimeException("Not supported node type:" + sourceNodeType);
        }
    }

    /**
     * 根据source节点类型（model或data）得到节点的配置信息，以供后续生成具体的node节点
     *
     * @param tableType 表类型：query_set或result_table
     * @param hdfsTableFormat hdfs上文件类型:parquet或iceberg
     * @param sourceNodeType 节点类型：model或data
     * @return 具体的配置信息，包括路径，存储等
     */
    public Map<String, Object> getConfigMap(ConstantVar.TableType tableType,
            ConstantVar.HdfsTableFormat hdfsTableFormat, ConstantVar.DataNodeType sourceNodeType) {
        Map<String, Object> inputInfo = (Map<String, Object>) this.nodeInfo.get("input");
        //公共配置
        Map<String, Object> sourceNodeConfMap = new HashMap<>();
        sourceNodeConfMap.put("id", this.nodeInfo.get("id").toString());
        sourceNodeConfMap.put("name", this.nodeInfo.get("name").toString());
        sourceNodeConfMap.put("job_type", jobType);
        sourceNodeConfMap.put("schedule_time", scheduleTime);
        sourceNodeConfMap.put("input", inputInfo);
        sourceNodeConfMap.put("fields", this.nodeInfo.get("fields"));

        if (sourceNodeType == DataNodeType.model) {
            sourceNodeConfMap.put("type", sourceNodeType.toString());
        }
        //补入iceberg信息
        if (hdfsTableFormat == HdfsTableFormat.iceberg) {
            //iceberg
            Map<String, Object> icebergConf = (Map<String, Object>) inputInfo.get("iceberg_config");
            Map<String, Object> icebergHdfsConf = (Map<String, Object>) icebergConf
                    .get("hdfs_config");
            Map<String, Object> finalIcebergConf = new HashMap<>();
            finalIcebergConf.put("hive.metastore.uris", icebergHdfsConf.get("hive.metastore.uris"));
            sourceNodeConfMap
                    .put("iceberg_table_name", icebergConf.get("physical_table_name").toString());
            sourceNodeConfMap
                    .put("iceberg_conf", finalIcebergConf);
        }

        //补入时间属性（queryset情况下填入默认值）
        if (tableType == TableType.query_set) {
            sourceNodeConfMap.put("start_time", -1L);
            sourceNodeConfMap.put("end_time", -1L);
            sourceNodeConfMap.put("type", sourceNodeType.toString());
        } else if (tableType == TableType.result_table) {
            // 周期执行任务或是dataflow上的调试任务
            Map<String, Object> windowInfo = (Map<String, Object>) this.nodeInfo.get("window");
            boolean isAccumulate = (boolean) windowInfo.get("accumulate");
            sourceNodeConfMap.put("accumulate", isAccumulate);
            if (isAccumulate) {
                sourceNodeConfMap
                        .put("data_start", Integer.parseInt(windowInfo.get("data_start").toString()));
                sourceNodeConfMap.put("data_end", Integer.parseInt(windowInfo.get("data_end").toString()));
            } else {
                sourceNodeConfMap
                        .put("window_size", Double.parseDouble(windowInfo.get("window_size").toString()));
                sourceNodeConfMap
                        .put("window_delay", Double.parseDouble(windowInfo.get("window_delay").toString()));
                sourceNodeConfMap
                        .put("window_size_period", windowInfo.get("window_size_period").toString());
            }
            sourceNodeConfMap.put("type", ConstantVar.Role.batch.toString());
        }
        return sourceNodeConfMap;
    }

    public void setDebugLabel(int debugLabel) {
        this.debugLabel = debugLabel;
    }
}
