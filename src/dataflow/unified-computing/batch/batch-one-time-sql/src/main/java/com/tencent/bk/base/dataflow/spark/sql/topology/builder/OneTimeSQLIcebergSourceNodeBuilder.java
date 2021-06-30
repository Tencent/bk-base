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

import com.tencent.bk.base.dataflow.spark.TimeFormatConverter$;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergInput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.SupportIcebergCount;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergSourceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OneTimeSQLIcebergSourceNodeBuilder extends BatchIcebergSourceNode.BatchIcebergSourceNodeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(OneTimeSQLIcebergSourceNodeBuilder.class);

    private List<SupportIcebergCount.TimeRangeTuple> timeRangeList;

    public OneTimeSQLIcebergSourceNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        this.timeRangeList = new LinkedList<>();

        Map<String, Object> partition = (Map<String, Object>)info.get("partition");
        try {
            buildTimeList(partition);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> input = (Map<String, Object>)info.get("input");
        Map<String, Object> inputConf = (Map<String, Object>)input.get("conf");

        String icebergTableName = inputConf.get("physical_table_name").toString();
        this.batchIcebergInput = new BatchIcebergInput(icebergTableName, this.timeRangeList);

        Map<String, Object> storekitHdfsConf = (Map<String, Object>)inputConf.get("storekit_hdfs_conf");
        this.icebergConf = new HashMap<>();
        this.icebergConf.put("hive.metastore.uris", storekitHdfsConf.get("hive.metastore.uris").toString());
        /*
        for(Map.Entry<String, Object> confItem : storekitHdfsConf.entrySet()) {
            Object value = CommonUtil$.MODULE$.formatNumberValueToLong(confItem.getValue());
            LOGGER.info(String.format("Set iceberg input conf: %s -> %s",
                    confItem.getKey(), value.toString()));
            this.icebergConf.put(confItem.getKey(), value.toString());
        }*/
    }

    @Override
    public BatchIcebergSourceNode build() {
        this.buildParams();
        return new BatchIcebergSourceNode(this);
    }

    private void buildTimeList(Map<String, Object> partition) throws ParseException {
        if (null != partition.get("list")) {
            for (String item : (List<String>)partition.get("list")) {
                long timeStamp = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(item).getTime();
                long endTime = timeStamp + 3600 * 1000L;
                SupportIcebergCount.TimeRangeTuple timeTuple =
                        new SupportIcebergCount.TimeRangeTuple(timeStamp, endTime);
                this.timeRangeList.add(timeTuple);
                LOGGER.info(String.format("Added single time to list %s -> %d", item, timeStamp));
            }
        }

        if (null != partition.get("range")) {
            for (Map<String, String> item : (List<Map<String, String>>)partition.get("range")) {
                long startTime = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(item.get("start")).getTime();
                long endTime = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(item.get("end")).getTime();
                //过滤条件为小于，不是小于等于，所以应该加一小时
                endTime = endTime + 3600 * 1000L;
                SupportIcebergCount.TimeRangeTuple timeTuple =
                        new SupportIcebergCount.TimeRangeTuple(startTime, endTime);
                this.timeRangeList.add(timeTuple);
                LOGGER.info(String.format("Added range time to list (%s, %s) -> (%d, %d)", item.get("start"),
                        item.get("end"), startTime, endTime));
            }
        }
    }
}
