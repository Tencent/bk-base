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

package com.tencent.bk.base.dataflow.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.spark.topology.nodes.BatchIcebergInput;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.AbstractBatchSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSCopySourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSCopySourceNode.BatchHDFSCopySourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchHDFSSourceNode.BatchHDFSSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergCopySourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergCopySourceNode.BatchIcebergCopySourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIcebergSourceNode.BatchIcebergSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIgniteSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchIgniteSourceNode.BatchIgniteSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchJsonSourceNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.source.BatchJsonSourceNode.BatchJsonSourceNodeBuilder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TopoSourceNodeTest {
    public String fakeHdfsRoot = "hdfs://fakeHdfs/api/flow";

    public static BatchHDFSSourceNode createSimpleHdfsSourceNode(
        String id,
        String root,
        String startTime,
        String endTime) throws ParseException {
        Map<String, Object> info = new HashMap<>();
        long startTimeMilli = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(startTime).getTime();
        long endTimeMilli = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(endTime).getTime();
        info.put("id", id);
        info.put("name",  id);
        info.put("start_time", startTimeMilli);
        info.put("end_time", endTimeMilli);
        BatchHDFSSourceNodeBuilder builder = new BatchHDFSSourceNodeBuilder(info);
        builder.setFieldConstructor(JavaTestUtils.getMockFieldConstructor(id));
        builder.setPathConstructor(JavaTestUtils.getMockHdfsPathConstructor(id, root));
        return builder.build();
    }

    public static BatchHDFSCopySourceNode createSimpleHdfsCopySourceNode(
        String id,
        String root,
        String startTime,
        String endTime) throws ParseException {
        Map<String, Object> info = new HashMap<>();
        long startTimeMilli = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(startTime).getTime();
        long endTimeMilli = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(endTime).getTime();
        info.put("id", id);
        info.put("name",  id);
        info.put("start_time", startTimeMilli);
        info.put("end_time", endTimeMilli);
        BatchHDFSCopySourceNodeBuilder builder = new BatchHDFSCopySourceNodeBuilder(info);
        builder.setFieldConstructor(JavaTestUtils.getMockFieldConstructor(id));
        builder.setPathConstructor(JavaTestUtils.getMockHdfsPathConstructor(id, root));
        return builder.build();
    }

    /**
     * 创建一个简单的IcebergSourceNode
     * @param id node id
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return
     */
    public static BatchIcebergSourceNode createSimpleIcebergSourceNode(
        String id,
        String startTime,
        String endTime) throws ParseException {
        Map<String, Object> info = new HashMap<>();
        long startTimeMilli = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(startTime).getTime();
        long endTimeMilli = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(endTime).getTime();
        info.put("id", id);
        info.put("name",  id);
        info.put("start_time", startTimeMilli);
        info.put("end_time", endTimeMilli);
        info.put("iceberg_table_name", String.format("iceberg.%s", id));

        Map<String, String> icebergConf = new HashMap<>();
        icebergConf.put("hive.metastore.uris", "fake_hive_url");
        info.put("iceberg_conf", icebergConf);
        BatchIcebergSourceNodeBuilder builder = new BatchIcebergSourceNodeBuilder(info);
        builder.setFieldConstructor(JavaTestUtils.getMockFieldConstructor(id));
        return builder.build();
    }

    /**
     * 创建一个简单的IcebergCopySourceNode
     * @param id node id
     * @param root 根目录
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return
     */
    public static BatchIcebergCopySourceNode createSimpleIcebergCopySourceNode(
        String id,
        String root,
        String startTime,
        String endTime) throws ParseException {
        Map<String, Object> info = new HashMap<>();
        long startTimeMilli = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(startTime).getTime();
        long endTimeMilli = TimeFormatConverter$.MODULE$.dataCTDateFormat().parse(endTime).getTime();
        info.put("id", id);
        info.put("name",  id);
        info.put("start_time", startTimeMilli);
        info.put("end_time", endTimeMilli);
        info.put("iceberg_table_name", String.format("iceberg.%s", id));

        Map<String, String> icebergConf = new HashMap<>();
        icebergConf.put("hive.metastore.uris", "fake_hive_url");
        info.put("iceberg_conf", icebergConf);
        BatchIcebergCopySourceNodeBuilder builder = new BatchIcebergCopySourceNodeBuilder(info);
        builder.setFieldConstructor(JavaTestUtils.getMockFieldConstructor(id));
        builder.setPathConstructor(JavaTestUtils.getMockHdfsPathConstructor(id, root));
        return builder.build();
    }

    public static BatchIgniteSourceNode createSimpleIgniteSourceNode(String id) {
        Map<String, Object> info = new HashMap<>();
        info.put("id", id);
        info.put("name",  id);
        BatchIgniteSourceNodeBuilder builder = new BatchIgniteSourceNodeBuilder(info);
        builder.setFieldConstructor(JavaTestUtils.getMockFieldConstructor(id));
        builder.setIgniteStorageConstructor(JavaTestUtils.getMockIgniteStorageConstructor(id));
        return builder.build();
    }

    /**
     * 创建一个简单的JsonSourceNode
     * @param id node id
     * @return
     */
    public static BatchJsonSourceNode createSimpleJsonSourceNode(String id) {
        Map<String, Object> info = new HashMap<>();
        info.put("id", id);
        info.put("name",  id);
        Map<String, Object> sourceData1 = new HashMap<>();
        sourceData1.put("id", "1");
        sourceData1.put("test_time_str", "2021-05-20 00:00:00");
        sourceData1.put("test_time_long", 1621440000000L);
        List<Map<String, Object>> sourceDataList = new ArrayList<>();
        sourceDataList.add(sourceData1);
        Map<String, Object> sourceData2 = new HashMap<>();
        sourceData2.put("id", "2");
        sourceData2.put("test_time_str", "2021-05-20 00:00:00");
        sourceData2.put("test_time_long", 1621440000000L);
        sourceDataList.add(sourceData2);
        Map<String, Object> sourceData3 = new HashMap<>();
        sourceData3.put("id", "3");
        sourceData3.put("test_time_str", "2021-05-20 00:00:00");
        sourceData3.put("test_time_long", 1621440000000L);
        sourceDataList.add(sourceData3);
        info.put("source_data",  sourceDataList);
        Map<String, Object> field1 = new HashMap<>();
        field1.put("field", "id");
        field1.put("type", "string");
        field1.put("origin", "");
        List<Map<String, Object>> serializedFields = new ArrayList<>();
        serializedFields.add(field1);
        Map<String, Object> field2 = new HashMap<>();
        field2.put("field", "test_time_str");
        field2.put("type", "string");
        field2.put("origin", "");
        serializedFields.add(field2);
        Map<String, Object> field3 = new HashMap<>();
        field3.put("field", "test_time_long");
        field3.put("type", "long");
        field3.put("origin", "");
        serializedFields.add(field3);
        info.put("fields", serializedFields);

        BatchJsonSourceNodeBuilder builder = new BatchJsonSourceNodeBuilder(info);
        return builder.build();
    }

    private void validateField(AbstractBatchSourceNode sourceNode) {
        List<NodeField> fieldList = sourceNode.getFields();
        Map<String, NodeField> fieldMap = new HashMap();
        for (NodeField field : fieldList) {
            fieldMap.put(field.getField(), field);
        }

        assertEquals(fieldList.size(), 3);
        NodeField field1 = fieldMap.get("id");
        assertEquals("id", field1.getField());
        assertEquals("String", field1.getType());
        NodeField field2 = fieldMap.get("test_time_str");
        assertEquals("test_time_str", field2.getField());
        assertEquals("String", field2.getType());
        NodeField field3 = fieldMap.get("test_time_long");
        assertEquals("test_time_long", field3.getField());
        assertEquals("long", field3.getType());
    }

    @Test
    public void testCreateJsonSourceNode() {
        String tableName = "json_source_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);

        BatchJsonSourceNode jsonSourceNode = TopoSourceNodeTest.createSimpleJsonSourceNode(resultTableId);

        assertEquals(resultTableId, jsonSourceNode.getNodeId());
        List<String> fieldList = new ArrayList<>();
        for (NodeField field : jsonSourceNode.getFields()) {
            fieldList.add(field.getField());
        }
        assertTrue(fieldList.contains("id"));
        assertTrue(fieldList.contains("test_time_str"));
        assertTrue(fieldList.contains("test_time_long"));

        assertEquals(3, jsonSourceNode.getJsonInput().getInputInfo().size());
        for (Map<String, Object> dataMap : jsonSourceNode.getJsonInput().getInputInfo()) {
            assertEquals(3, dataMap.size());
        }
    }

    @Test
    public void testCreateIgniteSourceNode() {
        String tableName = "ignite_source_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);

        BatchIgniteSourceNode igniteSourceNode = TopoSourceNodeTest.createSimpleIgniteSourceNode(resultTableId);

        assertEquals(resultTableId, igniteSourceNode.getPhysicalName());

        Map<String, Object> expectedConfMap = ScalaTestUtils$.MODULE$.getIgniteFakeConfMapAsJava();
        for (Map.Entry<String, Object> entry : igniteSourceNode.getConnectionInfo().entrySet()) {
            assertEquals(expectedConfMap.get(entry.getKey()).toString(), entry.getValue().toString());
        }
    }

    @Test
    public void testCreateIcebergSourceNode() throws ParseException {
        String tableName = "hdfs_source_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);

        BatchIcebergSourceNode icebergSourceNode = TopoSourceNodeTest.createSimpleIcebergSourceNode(
            resultTableId,
            "2021052200",
            "2021052300"
        );

        assertEquals(resultTableId, icebergSourceNode.getNodeId());
        this.validateField(icebergSourceNode);
        assertEquals("fake_hive_url", icebergSourceNode.getIcebergConf().get("hive.metastore.uris"));

        BatchIcebergInput input = icebergSourceNode.getBatchIcebergInput();
        assertEquals("iceberg.8_hdfs_source_node", input.getInputInfo());
        assertEquals(-1, input.getLimit());
        assertTrue(input.isIcebergOptimizedCountEnable());
        String expectedFilterExpression =
            "(____et >= cast(1621612800 as timestamp) AND ____et < cast(1621699200 as timestamp))";
        assertEquals(expectedFilterExpression, input.getFilterExpression());

        assertEquals("1621612800_1621699200", icebergSourceNode.getInputTimeRangeString());
    }

    @Test
    public void testCreateIcebergCopySourceNode() throws ParseException {
        String tableName = "hdfs_source_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);

        String root = String.format("%s/%s/%s_%s", this.fakeHdfsRoot, bizId, tableName, bizId);

        assertEquals("hdfs://fakeHdfs/api/flow/8/hdfs_source_node_8", root);

        BatchIcebergCopySourceNode icebergCopySourceNode = TopoSourceNodeTest.createSimpleIcebergCopySourceNode(
            resultTableId,
            root,
            "2021052200",
            "2021052300"
        );

        assertEquals(resultTableId, icebergCopySourceNode.getNodeId());
        this.validateField(icebergCopySourceNode);
        assertEquals("fake_hive_url", icebergCopySourceNode.getIcebergConf().get("hive.metastore.uris"));

        BatchIcebergInput input = icebergCopySourceNode.getBatchIcebergInput();
        assertEquals("iceberg.8_hdfs_source_node", input.getInputInfo());
        assertEquals(-1, input.getLimit());
        assertTrue(input.isIcebergOptimizedCountEnable());
        String expectedFilterExpression =
            "(____et >= cast(1621612800 as timestamp) AND ____et < cast(1621699200 as timestamp))";
        assertEquals(expectedFilterExpression, input.getFilterExpression());

        List<String> pathCopyList = icebergCopySourceNode.getHdfsCopyInput().getInputInfo();
        this.validatePathList(pathCopyList, String.format("%s/copy", root));

        assertEquals("1621612800_1621699200", icebergCopySourceNode.getInputTimeRangeString());
    }

    @Test
    public void testCreateHdfsSourceNode() throws ParseException {
        String tableName = "hdfs_source_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);
        String root = String.format("%s/%s/%s_%s", this.fakeHdfsRoot, bizId, tableName, bizId);

        assertEquals("hdfs://fakeHdfs/api/flow/8/hdfs_source_node_8", root);

        BatchHDFSSourceNode hdfsSourceNode = TopoSourceNodeTest.createSimpleHdfsSourceNode(
            resultTableId,
            root,
            "2021052200",
            "2021052300"
        );

        assertEquals(resultTableId, hdfsSourceNode.getNodeId());
        this.validateField(hdfsSourceNode);

        List<String> pathList = hdfsSourceNode.getHdfsInput().getInputInfo();
        this.validatePathList(pathList, root);

        assertEquals("1621612800_1621699200", hdfsSourceNode.getInputTimeRangeString());
    }

    @Test
    public void testCreateHdfsCopySourceNode() throws ParseException {
        String tableName = "hdfs_source_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);
        String root = String.format("%s/%s/%s_%s", this.fakeHdfsRoot, bizId, tableName, bizId);

        assertEquals("hdfs://fakeHdfs/api/flow/8/hdfs_source_node_8", root);

        BatchHDFSCopySourceNode hdfsCopySourceNode = TopoSourceNodeTest.createSimpleHdfsCopySourceNode(
            resultTableId,
            root,
            "2021052200",
            "2021052300"
        );

        assertEquals(resultTableId, hdfsCopySourceNode.getNodeId());
        this.validateField(hdfsCopySourceNode);

        List<String> pathList = hdfsCopySourceNode.getHdfsInput().getInputInfo();
        this.validatePathList(pathList, root);

        List<String> pathCopyList = hdfsCopySourceNode.getHdfsCopyInput().getInputInfo();
        this.validatePathList(pathCopyList, String.format("%s/copy", root));

        assertEquals("1621612800_1621699200", hdfsCopySourceNode.getInputTimeRangeString());
    }

    private void validatePathList(List<String> pathList, String root) {
        assertEquals(pathList.size(), 24);
        assertTrue(pathList.contains(String.format("%s/2021/05/22/00", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/01", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/02", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/03", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/04", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/05", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/06", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/07", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/08", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/09", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/10", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/11", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/12", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/13", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/14", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/15", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/16", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/17", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/18", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/19", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/20", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/21", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/22", root)));
        assertTrue(pathList.contains(String.format("%s/2021/05/22/23", root)));
    }
}
