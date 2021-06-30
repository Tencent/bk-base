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

import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.v2.io.InputWindowAnalyzer;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchPathListInput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class PeriodicSQLv2HDFSSourceNodeBuilder extends BatchHDFSSourceNode.BatchHDFSSourceNodeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicSQLv2HDFSSourceNodeBuilder.class);

    protected InputWindowAnalyzer windowAnalyzer;

    public PeriodicSQLv2HDFSSourceNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        this.isAccumulateNotInRange = false;
        this.fields = this.parseV2Field((List<Map<String, String>>)info.get("fields"));

        this.windowAnalyzer = new InputWindowAnalyzer(info);
        if (this.windowAnalyzer.isAccumulateNotInRange()) {
            LOGGER.warn(String.format("%s accumulate input time not in valid range", this.nodeId));
            this.isAccumulateNotInRange = true;
            this.inputTimeRangeString = "not_available";
            return;
        }

        LOGGER.info(String.format("Start to build path for %s", this.nodeId));

        String root = this.pathConstructor.getHdfsRoot((Map<String, String>)info.get("storage_conf"));

        List<String> pathList = this.pathConstructor.getAllTimePerHourAsJava(
            root,
            this.windowAnalyzer.getStartTimeInMilliseconds(),
            this.windowAnalyzer.getEndTimeInMilliseconds(),
            1);

        this.inputTimeRangeString = String.format("%d_%d", this.windowAnalyzer.getStartTimeInMilliseconds() / 1000,
            this.windowAnalyzer.getEndTimeInMilliseconds() / 1000);
        LOGGER.info(String.format("%s input time range %s", this.nodeId, this.inputTimeRangeString));

        this.hdfsInput = new BatchPathListInput(pathList);
    }

    public InputWindowAnalyzer getWindowAnalyzer() {
        return this.windowAnalyzer;
    }

    @Override
    public BatchHDFSSourceNode build() {
        this.buildParams();
        return new BatchHDFSSourceNode(this);
    }
}
