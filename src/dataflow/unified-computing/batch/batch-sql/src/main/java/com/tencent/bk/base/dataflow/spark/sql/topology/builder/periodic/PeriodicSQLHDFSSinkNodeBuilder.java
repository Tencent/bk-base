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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchSinglePathOutput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode.BatchHDFSSinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.helper.TopoBuilderScalaHelper$;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PeriodicSQLHDFSSinkNodeBuilder extends BatchHDFSSinkNodeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicSQLHDFSSinkNodeBuilder.class);
    protected Map<String, Object> storages;

    private int minWindowSize;
    private ConstantVar.PeriodUnit minWindowUnit;

    public PeriodicSQLHDFSSinkNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        this.storages = PeriodicSQLHDFSSinkNodeBuilder.getStorages(this.nodeId, this.info);
        this.scheduleTimeInHour =
            CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)this.info.get("schedule_time"));
        this.enableShipperInterface = PeriodicSQLHDFSSinkNodeBuilder.isDatabusShipperEnable(this.storages);
        this.minWindowSize = (int)this.info.get("min_window_size");
        this.minWindowUnit = (ConstantVar.PeriodUnit)this.info.get("min_window_unit");
        this.dtEventTimeStamp = TopoBuilderScalaHelper$.MODULE$.getDtEventTimeInMs(
            this.scheduleTimeInHour, this.minWindowSize, this.minWindowUnit);
        LOGGER.info(String.format("Builder dtEventTime %d", this.dtEventTimeStamp));
        this.hdfsOutput = buildHdfsOutput(this.storages);
    }

    private BatchSinglePathOutput buildHdfsOutput(Map<String, Object> storages) {
        String root = pathConstructor.getHdfsRoot(this.nodeId);
        String outputPath = pathConstructor.timeToPath(root, this.dtEventTimeStamp);
        LOGGER.info(String.format("Output path for %s: %s", this.nodeId, outputPath));

        this.outputTimeRangeString =
                String.format("%d_%d", this.dtEventTimeStamp / 1000, this.scheduleTimeInHour / 1000);
        LOGGER.info(String.format("%s Output time range %s", this.nodeId, this.outputTimeRangeString));
        String dataType = PeriodicSQLHDFSSinkNodeBuilder.parseDataType(storages);
        return new BatchSinglePathOutput(outputPath, dataType, "overwrite");
    }

    // 判断是否需要调用总线分发api
    public static boolean isDatabusShipperEnable(Map<String, Object> storages) {
        for (Map.Entry<String, Object> entry : storages.entrySet()) {
            if (!entry.getKey().equals("hdfs") && !entry.getKey().equals("kafka")) {
                return true;
            }
        }
        return false;
    }

    // 解析get_param中storages参数
    public static String parseDataType(Map<String, Object> storages) {
        /*String dataType = "parquet";
        if (null != storages.get("hdfs")) {
            Map<String, Object> hdfsConf = (Map<String, Object>)storages.get("hdfs");
            if (null != hdfsConf.get("data_type"))
                dataType = hdfsConf.get("data_type").toString();
        }
        return dataType;*/
        return "parquet";
    }


    // 解析get_param中storages参数
    public static Map<String, Object> getStorages(String resultTableId, Map<String, Object> info) {
        Map<String, Object> storages = (Map<String, Object>)info.get("storages");
        if (null != storages.get("multi_outputs")) {
            Map<String, Object> multiOutputs = (Map<String, Object>)storages.get("multi_outputs");
            storages = (Map<String, Object>)multiOutputs.get(resultTableId);
        }
        return storages;
    }

    @Override
    public BatchHDFSSinkNode build() {
        this.buildParams();
        BatchHDFSSinkNode sinkNode = new BatchHDFSSinkNode(this);
        sinkNode.setEnableReservedField(true);
        sinkNode.setEnableCallMaintainInterface(true);
        sinkNode.setEnableCallShipperInterface(this.enableShipperInterface);
        sinkNode.setForceToUpdateReservedSchema(false);
        return sinkNode;
    }
}
