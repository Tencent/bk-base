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

package com.tencent.bk.base.dataflow.spark.topology.nodes.sink;

import com.tencent.bk.base.dataflow.spark.topology.nodes.AbstractBatchNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergOutput;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BatchIcebergSinkNode extends AbstractBatchSinkNode {

    protected long dtEventTimeStamp;

    protected long scheduleTimeInHour;

    protected boolean enableIcebergPartitionColumn;

    protected BatchIcebergOutput batchIcebergOutput;
    protected Map<String, Object> icebergConf;

    public BatchIcebergSinkNode(BatchIcebergSinkNodeBuilder builder) {
        super(builder);
        this.batchIcebergOutput = builder.batchIcebergOutput;
        this.icebergConf = builder.icebergConf;

        this.dtEventTimeStamp = builder.dtEventTimeStamp;
        this.outputTimeRangeString = builder.outputTimeRangeString;
        this.scheduleTimeInHour = builder.scheduleTimeInHour;
        this.enableCallMaintainInterface = builder.enableMaintainInterface;
        this.enableCallShipperInterface = builder.enableShipperInterface;
        this.enableReservedField = builder.enableReservedSchema;
        this.forceUpdateReservedSchema = builder.forceUpdateReservedSchema;
        this.enableIcebergPartitionColumn = builder.enableIcebergPartitionColumn;
    }

    public long getDtEventTimeStamp() {
        return dtEventTimeStamp;
    }

    public BatchIcebergOutput getBatchIcebergOutput() {
        return batchIcebergOutput;
    }

    public Map<String, Object> getIcebergConf() {
        return this.icebergConf;
    }

    public long getScheduleTimeInHour() {
        return this.scheduleTimeInHour;
    }

    public boolean isEnableIcebergPartitionColumn() {
        return enableIcebergPartitionColumn;
    }

    public void setEnableIcebergPartitionColumn(boolean enableIcebergPartitionColumn) {
        this.enableIcebergPartitionColumn = enableIcebergPartitionColumn;
    }

    public static class BatchIcebergSinkNodeBuilder extends AbstractBatchNodeBuilder {
        private static final Logger LOGGER = LoggerFactory.getLogger(BatchIcebergSinkNodeBuilder.class);
        protected String outputTimeRangeString;
        protected long dtEventTimeStamp;
        protected long scheduleTimeInHour;

        protected BatchIcebergOutput batchIcebergOutput;
        protected Map<String, Object> icebergConf;

        protected boolean enableReservedSchema;
        protected boolean enableMaintainInterface;
        protected boolean enableShipperInterface;
        protected boolean forceUpdateReservedSchema;
        protected boolean enableIcebergPartitionColumn;

        public BatchIcebergSinkNodeBuilder(Map<String, Object> info) {
            super(info);
        }

        @Override
        public BatchIcebergSinkNode build() {
            this.buildParams();
            BatchIcebergSinkNode sinkNode = new BatchIcebergSinkNode(this);
            return sinkNode;
        }

        protected void buildParams() {
            this.dtEventTimeStamp = (long)this.retrieveRequiredString(this.info, "dt_event_time");

            if (null != this.info.get("enable_reserved_schema")) {
                this.enableReservedSchema = Boolean.parseBoolean(this.info.get("enable_reserved_schema").toString());
            } else {
                this.enableReservedSchema = false;
            }

            if (null != this.info.get("enable_maintain_api")) {
                this.enableMaintainInterface = Boolean.parseBoolean(this.info.get("enable_maintain_api").toString());
            } else {
                this.enableMaintainInterface = false;
            }

            if (null != this.info.get("enable_databus_shipper")) {
                this.enableShipperInterface = Boolean.parseBoolean(this.info.get("enable_databus_shipper").toString());
            } else {
                this.enableShipperInterface = false;
            }

            if (null != this.info.get("force_update_reserved_schema")) {
                this.forceUpdateReservedSchema =
                    Boolean.parseBoolean(this.info.get("force_update_reserved_schema").toString());
            } else {
                this.forceUpdateReservedSchema = false;
            }

            if (null != this.info.get("enable_iceberg_partition_column")) {
                this.enableIcebergPartitionColumn =
                    Boolean.parseBoolean(this.info.get("enable_iceberg_partition_column").toString());
            } else {
                this.enableIcebergPartitionColumn = false;
            }

            String icebergTableName = this.retrieveRequiredString(this.info,"iceberg_table_name").toString();
            String outputMode = this.retrieveRequiredString(this.info, "output_mode").toString();
            this.batchIcebergOutput = new BatchIcebergOutput(icebergTableName, outputMode, this.dtEventTimeStamp);
            LOGGER.info(String.format("Iceberg Output: table_name -> %s, mode -> %s, dtEventTimeStamp -> %d",
                icebergTableName, "overwrite", this.dtEventTimeStamp));
            this.icebergConf = (Map<String, Object>)this.retrieveRequiredString(this.info,"iceberg_conf");

            this.scheduleTimeInHour =
                CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)this.info.get("schedule_time"));
            this.outputTimeRangeString =
                String.format("%d_%d", this.dtEventTimeStamp / 1000, this.scheduleTimeInHour / 1000);
        }
    }
}
