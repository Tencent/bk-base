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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug;

import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergInput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergSourceNode.BatchIcebergSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.UCSparkConf$;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SQLDebugIcebergNodeBuilder extends BatchIcebergSourceNodeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLDebugIcebergNodeBuilder.class);

    public SQLDebugIcebergNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        this.fields = this.fieldConstructor.getRtField(this.nodeId);

        long scheduleTimeInHour =
            CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)info.get("schedule_time"));

        String icebergTableName = this.retrieveRequiredString(info,"iceberg_table_name").toString();
        long latestHourRange = UCSparkConf$.MODULE$.sqlDebugDataRange();
        long startTimeMills = scheduleTimeInHour - latestHourRange * 3600 * 1000L;
        long endTimeMills = scheduleTimeInHour + 3600 * 1000L;
        this.batchIcebergInput = new BatchIcebergInput(icebergTableName, startTimeMills, endTimeMills);
        this.icebergConf = (Map<String, Object>)this.retrieveRequiredString(info,"iceberg_conf");
        this.batchIcebergInput.setLimit(UCSparkConf$.MODULE$.sqlDebugDataLimit());
        LOGGER.info(String.format("Debug iceberg time range between %d and %d", startTimeMills, endTimeMills));
    }

    @Override
    public BatchIcebergSourceNode build() {
        this.buildParams();
        return new BatchIcebergSourceNode(this);
    }
}
