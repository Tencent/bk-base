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

import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchPathListInput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode.BatchHDFSSourceNodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.util.List;
import java.util.Map;

public class PeriodicSQLHDFSSourceNodeBuilder extends BatchHDFSSourceNodeBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicSQLHDFSSourceNodeBuilder.class);

    public PeriodicSQLHDFSSourceNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    public BatchHDFSSourceNode build() {
        this.buildParams();
        return new BatchHDFSSourceNode(this);
    }

    protected void buildParams() {
        this.fields = this.fieldConstructor.getRtField(this.nodeId);

        String root = this.pathConstructor.getHdfsRoot(this.nodeId);

        PeriodicSQLHDFSSourceParamHelper helper =
            new PeriodicSQLHDFSSourceParamHelper(info, this.pathConstructor, this.nodeId);
        LOGGER.info(String.format("Start to build path for %s", this.nodeId));
        Tuple3 pathTuple3 = helper.buildPathAndTimeRangeInput(root);

        this.inputTimeRangeString = String
            .format("%d_%d", (Long) pathTuple3._2() / 1000, (Long) pathTuple3._3() / 1000);
        LOGGER.info(String.format("%s input time range %s", this.nodeId, this.inputTimeRangeString));
        List<String> pathList = (List<String>) pathTuple3._1();
        this.hdfsInput = new BatchPathListInput(pathList);
    }
}
