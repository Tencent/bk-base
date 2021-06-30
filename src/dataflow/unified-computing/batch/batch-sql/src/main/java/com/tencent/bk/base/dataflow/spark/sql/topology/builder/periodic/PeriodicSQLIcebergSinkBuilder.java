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

import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergOutput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergSinkNode.BatchIcebergSinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.helper.TopoBuilderScalaHelper$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PeriodicSQLIcebergSinkBuilder extends BatchIcebergSinkNodeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicSQLIcebergSinkBuilder.class);
    protected Map<String, Object> storages;

    private int minWindowSize;
    private ConstantVar.PeriodUnit minWindowUnit;

    public PeriodicSQLIcebergSinkBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        this.storages = PeriodicSQLHDFSSinkNodeBuilder.getStorages(this.nodeId, this.info);
        this.scheduleTimeInHour =
            CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)this.info.get("schedule_time"));
        this.icebergConf = (Map<String, Object>)this.retrieveRequiredString(this.info,"iceberg_conf");
        this.enableShipperInterface = PeriodicSQLHDFSSinkNodeBuilder.isDatabusShipperEnable(this.storages);
        this.minWindowSize = (int)info.get("min_window_size");
        this.minWindowUnit = (ConstantVar.PeriodUnit)info.get("min_window_unit");
        this.dtEventTimeStamp = TopoBuilderScalaHelper$.MODULE$.getDtEventTimeInMs(
            this.scheduleTimeInHour, this.minWindowSize, this.minWindowUnit);
        String icebergTableName = this.retrieveRequiredString(this.info,"iceberg_table_name").toString();
        this.batchIcebergOutput = new BatchIcebergOutput(icebergTableName, "overwrite", this.dtEventTimeStamp);
        LOGGER.info(String.format("Iceberg Output: table_name -> %s, mode -> %s, dtEventTimeStamp -> %d",
            icebergTableName, "overwrite", this.dtEventTimeStamp));
        this.outputTimeRangeString = String.format("%d_%d",
            this.dtEventTimeStamp / 1000, this.scheduleTimeInHour / 1000);
    }

    @Override
    public BatchIcebergSinkNode build() {
        this.buildParams();
        BatchIcebergSinkNode sinkNode = new BatchIcebergSinkNode(this);
        sinkNode.setEnableReservedField(true);
        sinkNode.setEnableCallMaintainInterface(true);
        sinkNode.setEnableCallShipperInterface(this.enableShipperInterface);
        sinkNode.setEnableIcebergPartitionColumn(true);
        return sinkNode;
    }
}
