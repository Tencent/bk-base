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

import com.tencent.bk.base.dataflow.spark.sql.topology.builder.copy.CopyTopoBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLTopoBuilder;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology;
import com.tencent.bk.base.dataflow.spark.topology.BatchTopology.AbstractBatchTopologyBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.v2.PeriodicSQLv2TopoBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug.SQLDebugTopoBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.udfdebug.UDFDebugTopoBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BatchSQLTopologyBuilder extends AbstractBatchTopologyBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchSQLTopologyBuilder.class);

    AbstractBatchTopologyBuilder builder;

    /**
     * 创建离线计算拓扑
     * @param parameters 创建topo参数
     */
    public BatchSQLTopologyBuilder(Map<String, Object> parameters) {
        super(parameters);
        boolean isDebug = Boolean.parseBoolean(parameters.getOrDefault("debug", false).toString());
        if ("udf_debug".equals(parameters.get("run_mode").toString().toLowerCase())) {
            LOGGER.info("Start using udf debug topology builder");
            builder = new UDFDebugTopoBuilder(parameters);
        } else if (isDebug) {
            LOGGER.info("Start using sql debug topology builder");
            builder = new SQLDebugTopoBuilder(parameters);
        } else if ("data_makeup".equals(parameters.get("type").toString().toLowerCase())) {
            LOGGER.info("Start using copy topology builder");
            builder = new CopyTopoBuilder(parameters);
        } else if ("batch_sql_v2".equals(parameters.get("type").toString().toLowerCase())) {
            LOGGER.info("Start using periodic topology v2 builder");
            builder = new PeriodicSQLv2TopoBuilder(parameters);
        } else {
            LOGGER.info("Start using periodic topology builder");
            builder = new PeriodicSQLTopoBuilder(parameters);
        }
    }

    @Override
    public BatchTopology build() {
        return builder.build();
    }
}
