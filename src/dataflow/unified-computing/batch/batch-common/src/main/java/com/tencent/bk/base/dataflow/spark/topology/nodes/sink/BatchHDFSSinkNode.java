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

import com.tencent.bk.base.dataflow.spark.topology.HDFSPathConstructor;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchSinglePathOutput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.AbstractBatchNodeBuilder;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;

import java.util.Map;

public class BatchHDFSSinkNode extends AbstractBatchSinkNode {

    protected BatchSinglePathOutput hdfsOutput;
    protected long dtEventTimeStamp;
    protected long scheduleTimeInHour;

    public BatchHDFSSinkNode(BatchHDFSSinkNodeBuilder builder) {
        super(builder);
        this.hdfsOutput = builder.hdfsOutput;
        this.dtEventTimeStamp = builder.dtEventTimeStamp;
        this.outputTimeRangeString = builder.outputTimeRangeString;
        this.scheduleTimeInHour = builder.scheduleTimeInHour;
        this.enableCallMaintainInterface = builder.enableMaintainInterface;
        this.enableCallShipperInterface = builder.enableShipperInterface;
        this.enableReservedField = builder.enableReservedSchema;
        this.forceUpdateReservedSchema = builder.forceUpdateReservedSchema;
    }

    public BatchSinglePathOutput getHdfsOutput() {
        return hdfsOutput;
    }

    public long getDtEventTimeStamp() {
        return dtEventTimeStamp;
    }

    public long getScheduleTimeInHour() {
        return this.scheduleTimeInHour;
    }

    public static class BatchHDFSSinkNodeBuilder extends AbstractBatchNodeBuilder {

        protected BatchSinglePathOutput hdfsOutput;
        protected String outputTimeRangeString;
        protected long dtEventTimeStamp;
        protected long scheduleTimeInHour;

        protected HDFSPathConstructor pathConstructor;

        protected boolean enableReservedSchema;
        protected boolean enableMaintainInterface;
        protected boolean enableShipperInterface;
        protected boolean forceUpdateReservedSchema;

        public BatchHDFSSinkNodeBuilder(Map<String, Object> info) {
            super(info);
            this.pathConstructor = new HDFSPathConstructor();
        }

        @Override
        public BatchHDFSSinkNode build() {
            this.buildParams();
            BatchHDFSSinkNode sinkNode = new BatchHDFSSinkNode(this);
            return sinkNode;
        }

        protected void buildParams() {
            this.dtEventTimeStamp = (long)this.retrieveRequiredString(this.info, "dt_event_time");
            String outputFormat = this.retrieveRequiredString(this.info, "data_type").toString();
            String outputMode = this.retrieveRequiredString(this.info, "output_mode").toString();
            String root = this.pathConstructor.getHdfsRoot(this.nodeId);
            String outputPath = this.pathConstructor.timeToPath(root, this.dtEventTimeStamp);
            this.hdfsOutput = new BatchSinglePathOutput(outputPath, outputFormat, outputMode);

            this.scheduleTimeInHour =
                CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)this.info.get("schedule_time"));

            this.outputTimeRangeString =
                String.format("%d_%d", this.dtEventTimeStamp / 1000, this.scheduleTimeInHour / 1000);

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
        }

        public void setPathConstructor(HDFSPathConstructor constructor) {
            this.pathConstructor = constructor;
        }
    }
}
