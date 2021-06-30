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

import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchPathListInput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode.BatchHDFSSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.UCSparkConf$;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import com.tencent.bk.base.dataflow.spark.utils.HadoopUtil$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SQLDebugHDFSSourceNodeBuilder extends BatchHDFSSourceNodeBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLDebugHDFSSourceNodeBuilder.class);

    public SQLDebugHDFSSourceNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        this.fields = this.fieldConstructor.getRtField(this.nodeId);
        this.hdfsInput = new BatchPathListInput(buildDebugInputPaths(info));
    }

    private List<String> buildDebugInputPaths(Map<String, Object> info) {
        long scheduleTimeInHour =
                CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)info.get("schedule_time"));
        long latestRangeHour = UCSparkConf$.MODULE$.sqlDebugDataRange();
        long startTimeMills = scheduleTimeInHour - latestRangeHour * 3600 * 1000L;
        long endTimeMills = scheduleTimeInHour + 3600 * 1000L;

        String root = pathConstructor.getHdfsRoot(this.nodeId);

        List<String> paths = pathConstructor.getAllTimePerHourAsJava(root, startTimeMills, endTimeMills, 1);
        List<String> javaPaths = new LinkedList<>();
        javaPaths.addAll(paths);
        Collections.reverse(javaPaths);
        for (String path : javaPaths) {
            if (HadoopUtil$.MODULE$.hasDataFiles(path)) {
                LOGGER.info("SQL Debug use path : " + path);
                List<String> dataFilesList = new LinkedList<>();
                dataFilesList.add(path);
                return dataFilesList;
            }
        }
        LOGGER.info("Can't find parquet files in past 24 hours");
        return paths;
    }

    @Override
    public BatchHDFSSourceNode build() {
        this.buildParams();
        return new BatchHDFSSourceNode(this);
    }
}
