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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.v2;

import com.tencent.bk.base.dataflow.spark.BatchTimeDelta;
import com.tencent.bk.base.dataflow.spark.BatchTimeStamp;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchSinglePathOutput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode.BatchHDFSSinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PeriodicSQLv2HDFSSinkNodeBuilder  extends BatchHDFSSinkNodeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicSQLv2HDFSSinkNodeBuilder.class);

    public PeriodicSQLv2HDFSSinkNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        this.scheduleTimeInHour =
            CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)this.info.get("schedule_time"));
        this.enableShipperInterface = (boolean)this.info.get("call_databus_shipper");

        BatchTimeStamp scheduleTimeObj = new BatchTimeStamp(this.scheduleTimeInHour);
        BatchTimeDelta dataOffsetObj = new BatchTimeDelta((String)this.info.get("data_time_offset"));

        this.dtEventTimeStamp = scheduleTimeObj.minus(dataOffsetObj).getTimeInMilliseconds();
        LOGGER.info(String.format("Builder dtEventTime %d", this.dtEventTimeStamp));
        this.hdfsOutput = buildHdfsOutput((Map<String, String>)this.info.get("storage_conf"));
    }

    private BatchSinglePathOutput buildHdfsOutput(Map<String, String> storages) {
        String root = pathConstructor.getHdfsRoot(storages);
        String outputPath = pathConstructor.timeToPath(root, this.dtEventTimeStamp);
        LOGGER.info(String.format("Output path for %s: %s", this.nodeId, outputPath));

        this.outputTimeRangeString =
                String.format("%d_%d", this.dtEventTimeStamp / 1000, this.scheduleTimeInHour / 1000);
        LOGGER.info(String.format("%s Output time range %s", this.nodeId, this.outputTimeRangeString));

        String dataType = "parquet"; //Always use parquet to write directly into hdfs. Iceberg will use iceberg builder
        return new BatchSinglePathOutput(outputPath, dataType, "overwrite");
    }

    @Override
    public BatchHDFSSinkNode build() {
        this.buildParams();
        BatchHDFSSinkNode sinkNode = new BatchHDFSSinkNode(this);
        sinkNode.setEnableReservedField(true);
        sinkNode.setEnableCallMaintainInterface(true);
        sinkNode.setEnableCallShipperInterface(this.enableShipperInterface);
        sinkNode.setForceToUpdateReservedSchema(false);
        sinkNode.setEnableStartEndTimeField(this.isFieldContainStartEndTime());
        return sinkNode;
    }
}