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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.spark.TimeFormatConverter$;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergOutput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergSinkNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class OneTimeSQLIcebergSinkNodeBuilder extends BatchIcebergSinkNode.BatchIcebergSinkNodeBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(OneTimeSQLIcebergSinkNodeBuilder.class);

    private ConstantVar.Role role;

    public OneTimeSQLIcebergSinkNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {

        Map<String, Object> outputMap = (Map<String, Object>)this.info.get("output");
        Map<String, Object> storageMap = (Map<String, Object>)outputMap.get("conf");

        String mode = outputMap.get("mode").toString();

        String icebergTableName = storageMap.get("physical_table_name").toString();

        if (null != this.info.get("role")) {
            this.role = ConstantVar.Role.valueOf(this.info.get("role").toString().toLowerCase());
        }

        if (null != this.role
                && this.role == ConstantVar.Role.queryset) {
            this.batchIcebergOutput = new BatchIcebergOutput(icebergTableName, mode);
            this.batchIcebergOutput.setCountTotalValue(true);
        } else {
            String partition = info.get("partition").toString();
            try {
                this.dtEventTimeStamp = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(partition).getTime();
                this.batchIcebergOutput = new BatchIcebergOutput(icebergTableName, mode, this.dtEventTimeStamp);
                LOGGER.info(String.format("Iceberg output table name is: %s, mode: %s, dtEventTimeStamp: %d",
                        icebergTableName, mode, this.dtEventTimeStamp));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        Map<String, Object> storekitHdfsConf = (Map<String, Object>)storageMap.get("storekit_hdfs_conf");
        this.icebergConf = new HashMap<>();
        this.icebergConf.put("hive.metastore.uris", storekitHdfsConf.get("hive.metastore.uris").toString());
        /*
        for(Map.Entry<String, Object> confItem : storekitHdfsConf.entrySet()) {
            Object value = CommonUtil$.MODULE$.formatNumberValueToLong(confItem.getValue());
            LOGGER.info(String.format("Set iceberg output conf: %s -> %s",
                    confItem.getKey(), value.toString()));
            this.icebergConf.put(confItem.getKey(), value.toString());
        }*/
    }

    @Override
    public BatchIcebergSinkNode build() {
        this.buildParams();
        BatchIcebergSinkNode sinkNode = new BatchIcebergSinkNode(this);
        if (this.role != ConstantVar.Role.queryset) {
            sinkNode.setEnableCallMaintainInterface(true);
            sinkNode.setEnableReservedField(true);
            sinkNode.setForceToUpdateReservedSchema(true);
            sinkNode.setEnableIcebergPartitionColumn(true);
        }
        sinkNode.setEnableCallShipperInterface(false);
        return sinkNode;
    }
}
