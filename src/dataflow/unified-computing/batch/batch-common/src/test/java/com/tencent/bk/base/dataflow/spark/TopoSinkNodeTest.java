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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.tencent.bk.base.dataflow.spark.topology.nodes.SupportIcebergCount.TimeRangeTuple;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchDebugHTTPSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchDebugHTTPSinkNode.BatchDebugHTTPSinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSCopySinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSCopySinkNode.BatchHDFSCopySinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchHDFSSinkNode.BatchHDFSSinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergCopySinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergCopySinkNode.BatchIcebergCopySinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergSinkNode;
import com.tencent.bk.base.dataflow.spark.topology.nodes.sink.BatchIcebergSinkNode.BatchIcebergSinkNodeBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TopoSinkNodeTest {

    public String fakeHdfsRoot = "hdfs://fakeHdfs/api/flow";

    public static BatchDebugHTTPSinkNode createSimpleDebugHttpSinkNode(
            String id,
            int execId,
            boolean enableThrowException) {
        Map<String, Object> info = new HashMap<>();
        info.put("id", id);
        info.put("name",  id);
        info.put("debug_id",  id);
        info.put("execute_id",  execId);
        info.put("enable_throw_exception",  enableThrowException);
        BatchDebugHTTPSinkNodeBuilder builder = new BatchDebugHTTPSinkNodeBuilder(info);
        return builder.build();
    }

    public static class HdfsSinkOption {
        boolean enableCallMaintainInterface;
        boolean enableCallShipperInterface;
        boolean forceUpdateReservedSchema;
        boolean reservedFieldEnabled;
        boolean dispatchToStorage = false;

        long dtEventTimeStamp;
        long scheduleTime;

        public boolean isEnableCallMaintainInterface() {
            return enableCallMaintainInterface;
        }

        public void setEnableCallMaintainInterface(boolean enableCallMaintainInterface) {
            this.enableCallMaintainInterface = enableCallMaintainInterface;
        }

        public boolean isEnableCallShipperInterface() {
            return enableCallShipperInterface;
        }

        public void setEnableCallShipperInterface(boolean enableCallShipperInterface) {
            this.enableCallShipperInterface = enableCallShipperInterface;
        }

        public boolean isForceUpdateReservedSchema() {
            return forceUpdateReservedSchema;
        }

        public void setForceUpdateReservedSchema(boolean forceUpdateReservedSchema) {
            this.forceUpdateReservedSchema = forceUpdateReservedSchema;
        }

        public boolean isReservedFieldEnabled() {
            return reservedFieldEnabled;
        }

        public void setReservedFieldEnabled(boolean reservedFieldEnabled) {
            this.reservedFieldEnabled = reservedFieldEnabled;
        }

        public boolean isDispatchToStorage() {
            return dispatchToStorage;
        }

        public void setDispatchToStorage(boolean dispatchToStorage) {
            this.dispatchToStorage = dispatchToStorage;
        }

        public long getDtEventTimeStamp() {
            return dtEventTimeStamp;
        }

        public void setDtEventTimeStamp(long dtEventTimeStamp) {
            this.dtEventTimeStamp = dtEventTimeStamp;
        }

        public long getScheduleTime() {
            return scheduleTime;
        }

        public void setScheduleTime(long scheduleTime) {
            this.scheduleTime = scheduleTime;
        }

    }

    public static BatchHDFSSinkNode createSimpleHdfsSinkNode(
            String id,
            String root,
            String format,
            String outputMode,
            HdfsSinkOption hdfsSinkOption) {
        Map<String, Object> info = new HashMap<>();
        info.put("id", id);
        info.put("name",  id);
        info.put("data_type", format);
        info.put("output_mode",  outputMode);
        info.put("schedule_time", hdfsSinkOption.getScheduleTime());
        info.put("dt_event_time",  hdfsSinkOption.getDtEventTimeStamp());
        info.put("enable_reserved_schema",  hdfsSinkOption.isReservedFieldEnabled());
        info.put("enable_maintain_api",  hdfsSinkOption.isEnableCallMaintainInterface());
        info.put("enable_databus_shipper",  hdfsSinkOption.isEnableCallShipperInterface());
        info.put("force_update_reserved_schema",  hdfsSinkOption.isForceUpdateReservedSchema());
        BatchHDFSSinkNodeBuilder builder = new BatchHDFSSinkNodeBuilder(info);
        builder.setPathConstructor(JavaTestUtils.getMockHdfsPathConstructor(id, root));
        return builder.build();
    }

    public static BatchHDFSCopySinkNode createSimpleHdfsCopySinkNode(
            String id,
            String root,
            String format,
            String outputMode,
            HdfsSinkOption hdfsSinkOption) {
        Map<String, Object> info = new HashMap<>();
        info.put("id", id);
        info.put("name",  id);
        info.put("data_type", format);
        info.put("output_mode",  outputMode);
        info.put("schedule_time",  hdfsSinkOption.getScheduleTime());
        info.put("dt_event_time",  hdfsSinkOption.getDtEventTimeStamp());
        info.put("enable_reserved_schema",  hdfsSinkOption.isReservedFieldEnabled());
        info.put("enable_maintain_api",  hdfsSinkOption.isEnableCallMaintainInterface());
        info.put("enable_databus_shipper",  hdfsSinkOption.isEnableCallShipperInterface());
        info.put("force_update_reserved_schema",  hdfsSinkOption.isForceUpdateReservedSchema());
        info.put("dispatch_to_storage",  hdfsSinkOption.isDispatchToStorage());
        BatchHDFSCopySinkNodeBuilder builder = new BatchHDFSCopySinkNodeBuilder(info);
        builder.setPathConstructor(JavaTestUtils.getMockHdfsPathConstructor(id, root));
        return builder.build();
    }

    /**
     * 创建一个简单的iceberg sink node
     * @param id node id
     * @param icebergTableName iceberg表名
     * @param outputMode 写入模式
     * @param hdfsSinkOption hdfsSink配置项
     * @return
     */
    public static BatchIcebergSinkNode createSimpleIcebergSinkNode(
            String id,
            String icebergTableName,
            String outputMode,
            HdfsSinkOption hdfsSinkOption) {
        Map<String, Object> info = new HashMap<>();

        info.put("id", id);
        info.put("name",  id);
        info.put("output_mode",  outputMode);
        info.put("schedule_time",  hdfsSinkOption.getScheduleTime());
        info.put("dt_event_time",  hdfsSinkOption.getDtEventTimeStamp());
        info.put("iceberg_table_name",  icebergTableName);
        info.put("enable_reserved_schema",  hdfsSinkOption.isReservedFieldEnabled());
        info.put("enable_maintain_api",  hdfsSinkOption.isEnableCallMaintainInterface());
        info.put("enable_databus_shipper",  hdfsSinkOption.isEnableCallShipperInterface());
        info.put("force_update_reserved_schema",  hdfsSinkOption.isForceUpdateReservedSchema());
        Map<String, String> icebergConf = new HashMap<>();
        icebergConf.put("hive.metastore.uris", "fake_hive_url");
        info.put("iceberg_conf", icebergConf);
        BatchIcebergSinkNodeBuilder builder = new BatchIcebergSinkNodeBuilder(info);
        return builder.build();
    }

    /**
     * 创建一个简单的IcebergCopySinkNode
     * @param id node id
     * @param icebergTableName iceberg表名
     * @param root copy根目录
     * @param outputMode 写入模式
     * @param copyFormat copy根目录
     * @param hdfsSinkOption hdfsSink配置项
     * @return
     */
    public static BatchIcebergCopySinkNode createSimpleIcebergCopySinkNode(
            String id,
            String icebergTableName,
            String root,
            String outputMode,
            String copyFormat,
            HdfsSinkOption hdfsSinkOption) {
        Map<String, Object> info = new HashMap<>();
        info.put("id", id);
        info.put("name",  id);
        info.put("output_mode",  outputMode);
        info.put("copy_format",  copyFormat);
        info.put("schedule_time", hdfsSinkOption.getScheduleTime());
        info.put("dt_event_time",  hdfsSinkOption.getDtEventTimeStamp());
        info.put("iceberg_table_name",  icebergTableName);
        info.put("dispatch_to_storage",  hdfsSinkOption.isDispatchToStorage());
        info.put("enable_reserved_schema",  hdfsSinkOption.isReservedFieldEnabled());
        info.put("enable_maintain_api",  hdfsSinkOption.isEnableCallMaintainInterface());
        info.put("enable_databus_shipper",  hdfsSinkOption.isEnableCallShipperInterface());
        info.put("force_update_reserved_schema",  hdfsSinkOption.isForceUpdateReservedSchema());
        Map<String, String> icebergConf = new HashMap<>();
        icebergConf.put("hive.metastore.uris", "fake_hive_url");
        info.put("iceberg_conf", icebergConf);
        BatchIcebergCopySinkNodeBuilder builder = new BatchIcebergCopySinkNodeBuilder(info);
        builder.setPathConstructor(JavaTestUtils.getMockHdfsPathConstructor(id, root));
        return builder.build();
    }

    @Test
    public void testDebugHttpSinkNode() {
        String debugId = "test_debug";
        int execId = 123456;
        BatchDebugHTTPSinkNode httpSinkNode = TopoSinkNodeTest.createSimpleDebugHttpSinkNode(
            debugId,
            execId,
            true
        );

        assertEquals(debugId, httpSinkNode.getNodeId());
        assertEquals(debugId, httpSinkNode.getDebugId());
        assertEquals(execId, httpSinkNode.getExecuteId());
        assertEquals(5, httpSinkNode.getLimitValue());
        assertTrue(httpSinkNode.isEnableThrowException());
    }

    @Test
    public void testHdfsSinkNode() {
        String tableName = "hdfs_sink_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);
        String root = String.format("%s/%s/%s_%s", this.fakeHdfsRoot, bizId, tableName, bizId);
        String expectedFormat = "parquet";
        String expectedOutputMode = "overwrite";
        long expectedDtEventTime = 1621440000000L;
        long expectedScheduleTime = 1621526400000L;
        HdfsSinkOption options = new HdfsSinkOption();
        options.setEnableCallMaintainInterface(false);
        options.setEnableCallShipperInterface(false);
        options.setForceUpdateReservedSchema(false);
        options.setReservedFieldEnabled(true);
        options.setDtEventTimeStamp(expectedDtEventTime);
        options.setScheduleTime(expectedScheduleTime);
        BatchHDFSSinkNode hdfsSinkNode = TopoSinkNodeTest.createSimpleHdfsSinkNode(
            resultTableId,
            root,
            expectedFormat,
            expectedOutputMode,
            options
        );

        String expectedPath = String.format("%s/2021/05/20/00", root);
        assertEquals(expectedPath, hdfsSinkNode.getHdfsOutput().getOutputInfo());
        assertEquals(expectedFormat, hdfsSinkNode.getHdfsOutput().getFormat());
        assertEquals(expectedOutputMode, hdfsSinkNode.getHdfsOutput().getMode());
        assertEquals(resultTableId, hdfsSinkNode.getNodeId());
        assertEquals(expectedDtEventTime, hdfsSinkNode.getDtEventTimeStamp());
        assertEquals(expectedScheduleTime, hdfsSinkNode.getScheduleTimeInHour());
        assertEquals("1621440000_1621526400", hdfsSinkNode.getOutputTimeRangeString());

        assertFalse(hdfsSinkNode.isEnableCallMaintainInterface());
        assertFalse(hdfsSinkNode.isEnableCallShipperInterface());
        assertFalse(hdfsSinkNode.isForceUpdateReservedSchema());
        assertTrue(hdfsSinkNode.isReservedFieldEnabled());
    }

    @Test
    public void testHdfsCopySinkNode() {
        String tableName = "hdfs_sink_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);
        String root = String.format("%s/%s/%s_%s", this.fakeHdfsRoot, bizId, tableName, bizId);
        String expectedFormat = "parquet";
        String expectedOutputMode = "overwrite";
        long expectedDtEventTime = 1621440000000L;
        long expectedScheduleTime = 1621526400000L;

        HdfsSinkOption options = new HdfsSinkOption();
        options.setEnableCallMaintainInterface(false);
        options.setEnableCallShipperInterface(false);
        options.setForceUpdateReservedSchema(false);
        options.setReservedFieldEnabled(true);
        options.setDispatchToStorage(true);
        options.setDtEventTimeStamp(expectedDtEventTime);
        options.setScheduleTime(expectedScheduleTime);

        BatchHDFSCopySinkNode hdfsCopySinkNode = TopoSinkNodeTest.createSimpleHdfsCopySinkNode(
            resultTableId,
            root,
            expectedFormat,
            expectedOutputMode,
            options
        );

        String expectedPath = String.format("%s/2021/05/20/00", root);
        assertEquals(expectedPath, hdfsCopySinkNode.getHdfsOutput().getOutputInfo());
        assertEquals(expectedFormat, hdfsCopySinkNode.getHdfsOutput().getFormat());
        assertEquals(expectedOutputMode, hdfsCopySinkNode.getHdfsOutput().getMode());

        String expectedCopyPath = String.format("%s/copy/2021/05/20/00", root);
        assertEquals(expectedCopyPath, hdfsCopySinkNode.getHdfsCopyOutput().getOutputInfo());
        assertEquals(expectedFormat, hdfsCopySinkNode.getHdfsCopyOutput().getFormat());
        assertEquals(expectedOutputMode, hdfsCopySinkNode.getHdfsCopyOutput().getMode());
        assertEquals("1621440000_1621526400", hdfsCopySinkNode.getOutputTimeRangeString());

        assertEquals(resultTableId, hdfsCopySinkNode.getNodeId());
        assertEquals(expectedDtEventTime, hdfsCopySinkNode.getDtEventTimeStamp());
        assertEquals(expectedScheduleTime, hdfsCopySinkNode.getScheduleTimeInHour());
        assertTrue(hdfsCopySinkNode.isEnableMoveToOrigin());

        assertTrue(hdfsCopySinkNode.isEnableCallMaintainInterface());
        assertFalse(hdfsCopySinkNode.isEnableCallShipperInterface());
        assertFalse(hdfsCopySinkNode.isForceUpdateReservedSchema());
        assertTrue(hdfsCopySinkNode.isReservedFieldEnabled());
    }

    @Test
    public void testIcebergSinkNode() {
        String tableName = "hdfs_sink_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);
        String expectedOutputMode = "overwrite";
        String icebergTableName = String.format("iceberg.%s", resultTableId);
        long expectedDtEventTime = 1621440000000L;
        long expectedScheduleTime = 1621526400000L;

        HdfsSinkOption options = new HdfsSinkOption();
        options.setEnableCallMaintainInterface(false);
        options.setEnableCallShipperInterface(false);
        options.setForceUpdateReservedSchema(false);
        options.setReservedFieldEnabled(true);
        options.setDtEventTimeStamp(expectedDtEventTime);
        options.setScheduleTime(expectedScheduleTime);
        BatchIcebergSinkNode icebergSinkNode = TopoSinkNodeTest.createSimpleIcebergSinkNode(
            resultTableId,
            icebergTableName,
            expectedOutputMode,
            options
        );

        assertEquals(icebergTableName, icebergSinkNode.getBatchIcebergOutput().getOutputInfo());
        assertEquals(expectedOutputMode, icebergSinkNode.getBatchIcebergOutput().getMode());
        List<TimeRangeTuple> icebergTimeTuple =  icebergSinkNode.getBatchIcebergOutput().getTimeRangeList();
        assertEquals(1, icebergTimeTuple.size());
        assertEquals(1621440000000L, icebergTimeTuple.get(0).getStartTime());
        assertEquals(1621443600000L, icebergTimeTuple.get(0).getEndTime());

        assertEquals(resultTableId, icebergSinkNode.getNodeId());
        assertEquals(expectedDtEventTime, icebergSinkNode.getDtEventTimeStamp());
        assertEquals(expectedScheduleTime, icebergSinkNode.getScheduleTimeInHour());
        assertEquals("1621440000_1621526400", icebergSinkNode.getOutputTimeRangeString());

        assertFalse(icebergSinkNode.isEnableCallMaintainInterface());
        assertFalse(icebergSinkNode.isEnableCallShipperInterface());
        assertFalse(icebergSinkNode.isForceUpdateReservedSchema());
        assertTrue(icebergSinkNode.isReservedFieldEnabled());
    }

    @Test
    public void testIcebergCopySinkNode() {
        String tableName = "hdfs_sink_node";
        String bizId = "8";
        String resultTableId = String.format("%s_%s", bizId, tableName);
        String root = String.format("%s/%s/%s_%s", this.fakeHdfsRoot, bizId, tableName, bizId);
        String expectedOutputMode = "overwrite";
        String icebergTableName = String.format("iceberg.%s", resultTableId);
        String expectedCopyFormat = "parquet";
        long expectedDtEventTime = 1621440000000L;
        long expectedScheduleTime = 1621526400000L;

        HdfsSinkOption options = new HdfsSinkOption();
        options.setEnableCallMaintainInterface(false);
        options.setEnableCallShipperInterface(false);
        options.setForceUpdateReservedSchema(false);
        options.setReservedFieldEnabled(true);
        options.setDispatchToStorage(true);
        options.setDtEventTimeStamp(expectedDtEventTime);
        options.setScheduleTime(expectedScheduleTime);
        BatchIcebergCopySinkNode icebergCopySinkNode = TopoSinkNodeTest.createSimpleIcebergCopySinkNode(
            resultTableId,
            icebergTableName,
            root,
            expectedOutputMode,
            expectedCopyFormat,
            options
        );

        assertEquals(icebergTableName, icebergCopySinkNode.getBatchIcebergOutput().getOutputInfo());
        assertEquals(expectedOutputMode, icebergCopySinkNode.getBatchIcebergOutput().getMode());
        List<TimeRangeTuple> icebergTimeTuple =  icebergCopySinkNode.getBatchIcebergOutput().getTimeRangeList();
        assertEquals(1, icebergTimeTuple.size());
        assertEquals(1621440000000L, icebergTimeTuple.get(0).getStartTime());
        assertEquals(1621443600000L, icebergTimeTuple.get(0).getEndTime());

        String expectedCopyPath = String.format("%s/copy/2021/05/20/00", root);
        assertEquals(expectedCopyPath, icebergCopySinkNode.getHdfsCopyOutput().getOutputInfo());
        assertEquals(expectedOutputMode, icebergCopySinkNode.getHdfsCopyOutput().getMode());
        assertEquals(expectedCopyFormat, icebergCopySinkNode.getHdfsCopyOutput().getFormat());

        assertEquals(resultTableId, icebergCopySinkNode.getNodeId());
        assertEquals(expectedDtEventTime, icebergCopySinkNode.getDtEventTimeStamp());
        assertEquals(expectedScheduleTime, icebergCopySinkNode.getScheduleTimeInHour());
        assertEquals("1621440000_1621526400", icebergCopySinkNode.getOutputTimeRangeString());

        assertTrue(icebergCopySinkNode.isEnableMoveToOrigin());
        assertTrue(icebergCopySinkNode.isEnableCallMaintainInterface());
        assertFalse(icebergCopySinkNode.isEnableCallShipperInterface());
        assertFalse(icebergCopySinkNode.isForceUpdateReservedSchema());
        assertTrue(icebergCopySinkNode.isReservedFieldEnabled());
    }
}
