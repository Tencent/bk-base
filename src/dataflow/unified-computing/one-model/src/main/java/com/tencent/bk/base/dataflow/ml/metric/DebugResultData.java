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

package com.tencent.bk.base.dataflow.ml.metric;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.ml.topology.ModelTopology;
import com.tencent.bk.base.dataflow.metrics.BaseMetric;
import com.tencent.bk.base.dataflow.metrics.util.TimeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugResultData {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebugResultData.class);
    private Node node;
    private ModelTopology topology;

    public DebugResultData(Node node, ModelTopology topology) {
        this.node = node;
        this.topology = topology;
    }

    /**
     * 上报结果数据的样本数据
     *
     * @param dataset 需要上报的输入或输入结果集
     * @return 返回上报数据结构
     */
    public Map<String, Object> reportResult(Dataset<Row> dataset) {
        if (dataset != null) {
            List<Map<String, String>> resultList = new ArrayList<>();
            List<Row> rows = dataset.takeAsList(5);
            for (Row row : rows) {
                if (row.schema() == null) {
                    LOGGER.warn("Schema is null, skip the row");
                    continue;
                }
                String[] fieldNames = row.schema().fieldNames();
                Map<String, String> dataMap = new HashMap<>();
                LOGGER.info(this.topology.getDebugConfig().getDebugId());
                for (String field : fieldNames) {
                    LOGGER.info(field + ":" + row.getAs(field));
                    dataMap.put(field, row.getAs(field).toString());
                }
                resultList.add(dataMap);
            }
            return this.reportResultMap(resultList);
        }
        return null;
    }

    /**
     * 将输入数据上报到指定的地址
     *
     * @param resultList 需要上报的数据集合列表
     * @return 返回上报数据结构
     */
    public Map<String, Object> reportResultMap(List<Map<String, String>> resultList) {
        LOGGER.info("report url:" + this.topology.getDebugConfig().getDebugUrl());
        Map<String, Object> reportData = new HashMap<>();
        reportData.put("debug_date", new TimeUtils(null));
        reportData.put("thedate", new TimeUtils("yyyyMMddHHmmss"));
        reportData.put("debug_id", this.topology.getDebugConfig().getDebugId());
        reportData.put("job_id", this.topology.getDebugConfig().getDebugExecId());
        reportData.put("job_type", ConstantVar.Role.batch.toString());
        reportData.put("result_data", resultList);
        reportData.put("result_table_id", this.node.getNodeId());
        try {
            BaseMetric baseMetric = new BaseMetric(0,
                    topology.getDebugConfig().getDebugUrl(),
                    "result_data/", "http");
            baseMetric.literal("report_data").setLiteral(reportData);
            baseMetric.literal("data_type").setLiteral("result_data");
            Map<String, String> result = baseMetric.getMetricsStringAndReport();
            LOGGER.info("debug result data:" + result);
            baseMetric.close();
        } catch (NoSuchFieldError e) {
            LOGGER.info("Report error:", e);
        }
        return reportData;
    }

}
