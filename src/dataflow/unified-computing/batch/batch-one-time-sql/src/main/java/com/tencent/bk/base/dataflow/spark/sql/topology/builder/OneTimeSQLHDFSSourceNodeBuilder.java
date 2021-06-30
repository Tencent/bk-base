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

import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchPathListInput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OneTimeSQLHDFSSourceNodeBuilder extends BatchHDFSSourceNode.BatchHDFSSourceNodeBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(OneTimeSQLHDFSSourceNodeBuilder.class);

    public OneTimeSQLHDFSSourceNodeBuilder(Map<String, Object> info) {
        super(info);
    }

    @Override
    protected void buildParams() {
        Map<String, String> storageConf = (Map<String, String>)((Map<String, Object>)info.get("input")).get("conf");
        String nameService = storageConf.get("name_service");

        String physicalName;
        // 如果hdfs存储格式为iceberg情况下，physical_table_name将不再是路径而是iceberg表名，原有hdfs路径将为hdfs_root_path
        if (null != storageConf.get("data_type") && storageConf.get("data_type").toLowerCase().equals("iceberg")) {
            physicalName = storageConf.get("hdfs_root_path");
        } else {
            physicalName = storageConf.get("physical_table_name");
        }

        String root = String.format("%s%s", nameService, physicalName);
        Map<String, Object> partition = (Map<String, Object>)info.get("partition");
        List<String> pathList = generateHdfsPath(partition, root);
        this.hdfsInput = new BatchPathListInput(pathList);
        LOGGER.info(String.format("Input paths is: %s",
            this.hdfsInput.getInputInfo().toString()));
    }

    private List<String> generateHdfsPath(Map<String, Object> partition, String root) {
        LinkedHashSet<String> pathHashSet = new LinkedHashSet<>();
        if (null != partition.get("list")) {
            for (String item : (List<String>)partition.get("list")) {
                pathHashSet.add(pathConstructor.timeToPath(root, item));
            }
        }

        if (null != partition.get("range")) {
            for (Map<String, String> item : (List<Map<String, String>>)partition.get("range")) {
                pathHashSet.addAll(pathConstructor.getAllTimePerHourToEndAsJava(root, item.get("start"),
                        item.get("end"), 1));
            }
        }
        return new LinkedList<>(pathHashSet);
    }

    @Override
    public BatchHDFSSourceNode build() {
        this.buildParams();
        return new BatchHDFSSourceNode(this);
    }
}
