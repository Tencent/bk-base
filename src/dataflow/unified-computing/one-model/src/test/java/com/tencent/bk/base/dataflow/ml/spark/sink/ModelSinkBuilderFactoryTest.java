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

package com.tencent.bk.base.dataflow.ml.spark.sink;

import static org.mockito.ArgumentMatchers.anyString;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataNodeType;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.HdfsTableFormat;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.PeriodUnit;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.TableType;
import com.tencent.bk.base.dataflow.core.topo.Node.AbstractBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSinkNode.ModelDefaultSinkNodeBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelIceBergQuerySetSinkNode.ModelIceBergQuerySetSinkNodeBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelSinkBuilderFactory;
import com.tencent.bk.base.dataflow.ml.spark.utils.FileUtil;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLHDFSSinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLIcebergSinkBuilder;
import com.tencent.bk.base.dataflow.spark.topology.HDFSPathConstructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.Tuple2;

@RunWith(PowerMockRunner.class)
@PrepareForTest(HDFSPathConstructor.class)
public class ModelSinkBuilderFactoryTest {

    private ModelSinkBuilderFactory factory;
    private Map<String, Object> sinkNodeInfo;
    private Map<String, Object> sinkNodeOutput;

    private void mockRTHdfsInfo() {

        try {
            HDFSPathConstructor pathConstructor = PowerMockito.mock(HDFSPathConstructor.class);
            PowerMockito.whenNew(HDFSPathConstructor.class).withNoArguments().thenReturn(pathConstructor);
            PowerMockito.when(pathConstructor.getHdfsRoot(anyString())).thenReturn("hdfs://xxxx/");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, Object> getIcebergConf() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> hdfsConfig = new HashMap<>();
        hdfsConfig.put("hive.metastore.uris", "test.uris");
        map.put("physical_table_name", "test_table");
        map.put("hdfs_config", hdfsConfig);
        return map;
    }

    private void init(String file) {
        try {
            String content = FileUtil.read(file);
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> parameters = objectMapper.readValue(content, HashMap.class);
            Map<String, Map<String, Object>> nodes = (Map<String, Map<String, Object>>) parameters.get("nodes");
            Map<String, Object> sourceNodes = nodes.get("source");
            Map<String, Object> sinkNodes = nodes.get("sink");
            long scheduleTime = (long) parameters.get("schedule_time");
            this.factory = new ModelSinkBuilderFactory("spark_mllib", scheduleTime, sourceNodes);
            this.sinkNodeInfo = (Map<String, Object>) sinkNodes.get("591_random_forest_result");
            this.sinkNodeOutput = (Map<String, Object>) sinkNodeInfo.get("output");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @PrepareForTest({HDFSPathConstructor.class, PeriodicSQLHDFSSinkNodeBuilder.class,
            ModelDefaultSinkNodeBuilder.class, PeriodicSQLIcebergSinkBuilder.class,
            ModelIceBergQuerySetSinkNodeBuilder.class})
    public void testGetBuilder() {
        try {
            this.mockRTHdfsInfo();
            this.init("/common_node_topology/test_periodic_topology.conf");

            // result_table + parquet
            AbstractBuilder builder = factory.getBuilder(this.sinkNodeInfo);
            assert builder instanceof PeriodicSQLHDFSSinkNodeBuilder;

            //queryset + parquet
            this.sinkNodeInfo.put("table_type", TableType.query_set.toString());
            builder = factory.getBuilder(this.sinkNodeInfo);
            assert builder instanceof ModelDefaultSinkNodeBuilder;

            //result_table + iceberg
            this.sinkNodeInfo.put("table_type", TableType.result_table.toString());
            sinkNodeOutput.put("format", "iceberg");
            sinkNodeOutput.put("iceberg_config", this.getIcebergConf());
            builder = factory.getBuilder(this.sinkNodeInfo);
            assert builder instanceof PeriodicSQLIcebergSinkBuilder;

            //queryset + iceberg
            this.sinkNodeInfo.put("table_type", TableType.query_set.toString());
            sinkNodeOutput.put("format", "iceberg");
            sinkNodeOutput.put("iceberg_config", this.getIcebergConf());
            builder = factory.getBuilder(this.sinkNodeInfo);
            assert builder instanceof ModelIceBergQuerySetSinkNodeBuilder;

            // model
            this.sinkNodeInfo.put("type", DataNodeType.model.toString());
            builder = factory.getBuilder(this.sinkNodeInfo);
            assert builder instanceof ModelDefaultSinkNodeBuilder;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @PrepareForTest({HDFSPathConstructor.class, PeriodicSQLHDFSSinkNodeBuilder.class,
            ModelDefaultSinkNodeBuilder.class, PeriodicSQLIcebergSinkBuilder.class,
            ModelIceBergQuerySetSinkNodeBuilder.class})
    public void testGetConfigMap() {
        this.init("/common_node_topology/test_periodic_topology.conf");
        this.mockRTHdfsInfo();
        // result_table + parquet
        factory.getBuilder(this.sinkNodeInfo);
        Map<String, Object> configMap = factory
                .getConfigMap(TableType.query_set, HdfsTableFormat.parquet, DataNodeType.model);
        assert !configMap.containsKey("iceberg_table_name");
        assert configMap.containsKey("min_window_size");
        assert "batch".equalsIgnoreCase(configMap.get("type").toString());

        //queryset + parquet
        this.sinkNodeInfo.put("table_type", TableType.query_set.toString());
        factory.getBuilder(this.sinkNodeInfo);
        configMap = factory
                .getConfigMap(TableType.query_set, HdfsTableFormat.parquet, DataNodeType.model);
        assert Long.parseLong(configMap.get("dt_event_time").toString()) == 0L;
        assert "query_set".equalsIgnoreCase(configMap.get("type").toString());

        //result_table + iceberg
        this.sinkNodeInfo.put("table_type", TableType.result_table.toString());
        sinkNodeOutput.put("format", "iceberg");
        sinkNodeOutput.put("iceberg_config", this.getIcebergConf());
        factory.getBuilder(this.sinkNodeInfo);
        configMap = factory
                .getConfigMap(TableType.query_set, HdfsTableFormat.parquet, DataNodeType.model);
        assert configMap.containsKey("iceberg_table_name");
        assert "batch".equalsIgnoreCase(configMap.get("type").toString());

        //queryset + iceberg
        this.sinkNodeInfo.put("table_type", TableType.query_set.toString());
        sinkNodeOutput.put("iceberg_config", this.getIcebergConf());
        factory.getBuilder(this.sinkNodeInfo);
        configMap = factory
                .getConfigMap(TableType.query_set, HdfsTableFormat.parquet, DataNodeType.model);
        assert Long.parseLong(configMap.get("dt_event_time").toString()) == 0L;

        // model
        this.sinkNodeInfo.put("type", DataNodeType.model.toString());
        factory.getBuilder(this.sinkNodeInfo);
        configMap = factory
                .getConfigMap(TableType.query_set, HdfsTableFormat.parquet, DataNodeType.model);
        assert "model".equalsIgnoreCase(configMap.get("type").toString());

    }

    @Test
    public void testGetNonAccumulateWindowSize() {
        this.init("/common_node_topology/test_periodic_topology.conf");
        Map<String, Object> windowInfo = new HashMap<>();
        windowInfo.put("window_size", 3);
        windowInfo.put("window_size_period", "hour");
        assert this.factory.getNonAccumulateWindowSize(windowInfo) == 3;
        windowInfo.put("window_size_period", "day");
        assert this.factory.getNonAccumulateWindowSize(windowInfo) == 3 * 24;
        windowInfo.put("window_size_period", "week");
        assert this.factory.getNonAccumulateWindowSize(windowInfo) == 7 * 3 * 24;
        windowInfo.put("window_size_period", "month");
        assert this.factory.getNonAccumulateWindowSize(windowInfo) == 3;
        windowInfo.put("window_size_period", "unknown");
        assert this.factory.getNonAccumulateWindowSize(windowInfo) == 1;
    }

    @Test
    public void testDealWithMinus1WindowSize() {
        this.init("/common_node_topology/test_periodic_topology.conf");
        List<Integer> windowSizeCollect = new ArrayList<>();
        ConstantVar.PeriodUnit scheduleUnit = PeriodUnit.hour;
        int countFreq = 3;
        ConstantVar.PeriodUnit unit = PeriodUnit.hour;
        ConstantVar.PeriodUnit newUnit = this.factory
                .dealWithMinus1WindowSize(scheduleUnit, countFreq, windowSizeCollect, unit);
        assert windowSizeCollect.get(0) == countFreq;
        assert newUnit == unit;

        scheduleUnit = PeriodUnit.day;
        newUnit = this.factory
                .dealWithMinus1WindowSize(scheduleUnit, countFreq, windowSizeCollect, unit);
        assert windowSizeCollect.get(0) == 24 * countFreq;
        assert newUnit == unit;

        scheduleUnit = PeriodUnit.week;
        newUnit = this.factory
                .dealWithMinus1WindowSize(scheduleUnit, countFreq, windowSizeCollect, unit);
        assert windowSizeCollect.get(0) == 24 * 7 * countFreq;
        assert newUnit == unit;

        scheduleUnit = PeriodUnit.month;
        newUnit = this.factory
                .dealWithMinus1WindowSize(scheduleUnit, countFreq, windowSizeCollect, unit);
        assert windowSizeCollect.get(0) == countFreq;
        assert newUnit == PeriodUnit.month;
    }

    @Test
    public void testGetWindowSize() {
        this.init("/common_node_topology/test_multi_input_output_topology.conf");
        Tuple2 tuple = factory.getWindowSize();
        assert (int) tuple._1 == 1;
        assert (ConstantVar.PeriodUnit) tuple._2 == PeriodUnit.hour;
    }
}
