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
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.PeriodUnit;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.TableType;
import com.tencent.bk.base.dataflow.core.topo.Node.AbstractBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLHDFSSinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLIcebergSinkBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.helper.TopoBuilderScalaHelper$;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import scala.Tuple2;

public class ModelSinkBuilderFactory extends AbstractModelBuilderFactory {

    private Map<String, Object> sourceNodes;

    public ModelSinkBuilderFactory(String jobType, long scheduleTime, Map<String, Object> sourceNodes) {
        super(jobType, scheduleTime);
        this.sourceNodes = sourceNodes;
    }

    @Override
    public AbstractBuilder getBuilder(Map<String, Object> sinkNodeInfo) {
        this.nodeInfo = sinkNodeInfo;
        //获取基本信息
        ConstantVar.DataNodeType sinkNodeType = ConstantVar.DataNodeType
                .valueOf(this.nodeInfo.get("type").toString());
        Map<String, Object> outputInfo = (Map<String, Object>) this.nodeInfo.get("output");
        ConstantVar.TableType tableType = ConstantVar.TableType.valueOf(outputInfo
                .getOrDefault("table_type", TableType.other.toString()).toString());
        ConstantVar.HdfsTableFormat hdfsTableFormat = getHdfsTableFormat(outputInfo.get("format").toString());
        Map<String, Object> sinkConfMap = this.getConfigMap(tableType, hdfsTableFormat, sinkNodeType);
        switch (sinkNodeType) {
            case model:
                return new ModelDefaultSinkNode.ModelDefaultSinkNodeBuilder(sinkConfMap);
            case data:
                switch (tableType) {
                    case query_set:
                        switch (hdfsTableFormat) {
                            case iceberg:
                                return new ModelIceBergQuerySetSinkNode.ModelIceBergQuerySetSinkNodeBuilder(
                                        sinkConfMap);
                            case parquet:
                                return new ModelDefaultSinkNode.ModelDefaultSinkNodeBuilder(sinkConfMap);
                            default:
                                throw new RuntimeException("Not supported table format:" + hdfsTableFormat);
                        }
                    case result_table:
                        switch (hdfsTableFormat) {
                            case parquet:
                                return new PeriodicSQLHDFSSinkNodeBuilder(sinkConfMap);
                            case iceberg:
                                return new PeriodicSQLIcebergSinkBuilder(sinkConfMap);
                            default:
                                throw new RuntimeException("Not supported table type:" + tableType);
                        }
                    default:
                        throw new RuntimeException("Not supported table type:" + hdfsTableFormat);
                }
            default:
                throw new RuntimeException("Not supported sink node type:" + sinkNodeType);
        }
    }

    /**
     * 根据sink节点类型（model或data）得到节点的配置信息，以供后续生成具体的node节点
     *
     * @param tableType 表类型：query_set或result_table
     * @param hdfsTableFormat hdfs上文件类型:parquet或iceberg
     * @param sinkNodeType 节点类型：model或data
     * @return 具体的配置信息，包括路径，存储等
     */
    @Override
    public Map<String, Object> getConfigMap(TableType tableType, HdfsTableFormat hdfsTableFormat,
            DataNodeType sinkNodeType) {
        Map<String, Object> sinkNodeConfMap = new HashMap<>();
        // 加入基本信息
        Map<String, Object> outputInfo = (Map<String, Object>) this.nodeInfo.get("output");
        String sinkNodeId = this.nodeInfo.get("id").toString();
        sinkNodeConfMap.put("id", sinkNodeId);
        sinkNodeConfMap.put("name", sinkNodeId);
        sinkNodeConfMap.put("job_type", jobType);
        sinkNodeConfMap.put("storages", (Map<String, Object>) this.nodeInfo.get("storages"));
        sinkNodeConfMap.put("schedule_time", scheduleTime);
        sinkNodeConfMap.put("output_mode", outputInfo.get("mode").toString());
        sinkNodeConfMap.put("fields", this.nodeInfo.get("fields"));
        sinkNodeConfMap.put("output", outputInfo);

        if (sinkNodeType == DataNodeType.model) {
            sinkNodeConfMap.put("type", sinkNodeType.toString());
        }
        //加入iceberg的信息
        if (hdfsTableFormat == HdfsTableFormat.iceberg) {
            Map<String, Object> icebergConf = (Map<String, Object>) outputInfo.get("iceberg_config");
            // todo:为规避出错，暂时只取hdfs_config中的hive.metastore.uris
            Map<String, Object> icebergHdfsConf = (Map<String, Object>) icebergConf.get("hdfs_config");
            Map<String, Object> finalIcebergConf = new HashMap<>();
            finalIcebergConf.put("hive.metastore.uris", icebergHdfsConf.get("hive.metastore.uris"));
            sinkNodeConfMap.put("iceberg_table_name", icebergConf.get("physical_table_name").toString());
            sinkNodeConfMap.put("iceberg_conf", finalIcebergConf);
        }
        //加入时间属性
        if (tableType == TableType.query_set) {
            sinkNodeConfMap.put("dt_event_time", 0L);
            sinkNodeConfMap.put("type", sinkNodeType.toString());
        } else if (tableType == TableType.result_table) {
            // 加入窗口信息
            Tuple2 minWindow = this.getWindowSize();
            int minWindowSize = ((Integer) minWindow._1()).intValue();
            ConstantVar.PeriodUnit minWindowUnit = (ConstantVar.PeriodUnit) minWindow._2();
            sinkNodeConfMap.put("min_window_size", minWindowSize);
            sinkNodeConfMap.put("min_window_unit", minWindowUnit);
            sinkNodeConfMap.put("type", ConstantVar.Role.batch.toString());
            long scheduleTimeInHour = CommonUtil$.MODULE$.roundScheduleTimeStampToHour(scheduleTime);
            long dtEventTimeStamp = TopoBuilderScalaHelper$.MODULE$.getDtEventTimeInMs(
                    scheduleTimeInHour, minWindowSize, minWindowUnit);
            sinkNodeConfMap.put("dt_event_time", dtEventTimeStamp);
        }
        return sinkNodeConfMap;
    }

    /**
     * 计算得到非累加窗口（滚动窗口）的窗口大小
     *
     * @param windowInfo 窗口信息
     * @return 返回计算得到的窗口大小
     */
    public int getNonAccumulateWindowSize(Map<String, Object> windowInfo) {
        int windowSize;
        ConstantVar.PeriodUnit windowSizePeriod = ConstantVar.PeriodUnit
                .valueOf(windowInfo.get("window_size_period").toString().toLowerCase());
        switch (windowSizePeriod) {
            case hour:
                windowSize = Double.valueOf(windowInfo.get("window_size").toString()).intValue();
                break;
            case day:
                windowSize = 24 * Double.valueOf(windowInfo.get("window_size").toString()).intValue();
                break;
            case week:
                windowSize = 7 * 24 * Double.valueOf(windowInfo.get("window_size").toString()).intValue();
                break;
            case month:
                windowSize = Double.valueOf(windowInfo.get("window_size").toString()).intValue();
                break;
            default:
                windowSize = 1;
        }
        return windowSize;
    }

    /**
     * 返回需要减小1时的窗口信息
     *
     * @param scheduleUnit 调度周期
     * @param countFreq 调度频率
     * @param unit 调度单位
     * @param windowSizeCollect 用于存储windowsize的列表
     * @return 返回计算后的调度单位
     */
    public ConstantVar.PeriodUnit dealWithMinus1WindowSize(ConstantVar.PeriodUnit scheduleUnit, int countFreq,
            List<Integer> windowSizeCollect, ConstantVar.PeriodUnit unit) {
        int windowSize = 1;
        switch (scheduleUnit) {
            case hour:
                windowSize = countFreq;
                break;
            case day:
                windowSize = 24 * countFreq;
                break;
            case week:
                windowSize = 7 * 24 * countFreq;
                break;
            case month:
                unit = ConstantVar.PeriodUnit.month;
                windowSize = countFreq;
                break;
            default:
                windowSize = countFreq;
                break;
        }
        windowSizeCollect.add(windowSize);
        return unit;
    }

    /**
     * 根据source内所有输入表的窗口信息，获取最小窗口
     */
    public Tuple2 getWindowSize() {
        List<Integer> windowSizeCollect = new ArrayList<>();
        boolean hasMinus1WindowSize = false;
        //父表调度单位
        ConstantVar.PeriodUnit unit = ConstantVar.PeriodUnit.hour;
        // 当前表调度单位
        ConstantVar.PeriodUnit scheduleUnit = ConstantVar.PeriodUnit.hour;
        int countFreq = 1;
        for (Map.Entry<String, Object> table : sourceNodes.entrySet()) {
            String sourceType = ((Map<String, Object>) table.getValue()).get("type").toString();
            if (!"data".equalsIgnoreCase(sourceType)) {
                continue;
            }
            Map<String, Object> windowInfo = (Map<String, Object>) ((Map<String, Object>) table.getValue())
                    .get("window");
            // 其实每个table内的schedule_period都是一样的
            scheduleUnit = ConstantVar.PeriodUnit.valueOf(windowInfo.get("schedule_period").toString().toLowerCase());
            countFreq = Integer.parseInt(windowInfo.get("count_freq").toString());
            int windowSize;
            Boolean accumuate = Boolean.valueOf(windowInfo.get("accumulate").toString());
            if (accumuate) {
                windowSize = 1;
            } else {
                windowSize = this.getNonAccumulateWindowSize(windowInfo);
                ConstantVar.PeriodUnit windowSizePeriod = ConstantVar.PeriodUnit
                        .valueOf(windowInfo.get("window_size_period").toString().toLowerCase());
                if (windowSizePeriod == PeriodUnit.month) {
                    unit = ConstantVar.PeriodUnit.month;
                }
            }
            if (windowSize == -1) {
                hasMinus1WindowSize = true;
            } else {
                windowSizeCollect.add(windowSize);
            }
        }
        if (hasMinus1WindowSize && windowSizeCollect.isEmpty()) {
            unit = this.dealWithMinus1WindowSize(scheduleUnit, countFreq,
                    windowSizeCollect, unit);
        }
        return new Tuple2(Collections.min(windowSizeCollect), unit);
    }
}
