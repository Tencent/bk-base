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

package com.tencent.bk.base.dataflow.ml.spark.source;

import static org.mockito.ArgumentMatchers.anyString;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.topo.Node.AbstractBuilder;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSourceNode.ModelDefaultSourceNodeBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelIceBergQuerySetSourceNode.ModelIceBergQuerySetSourceNodeBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelSourceBuilderFactory;
import com.tencent.bk.base.dataflow.ml.spark.utils.FileUtil;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLHDFSSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLHDFSSourceParamHelper;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLIcebergSourceBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug.SQLDebugHDFSSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug.SQLDebugIcebergNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.BkFieldConstructor;
import com.tencent.bk.base.dataflow.spark.utils.APIUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.Tuple3;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BkFieldConstructor.class, APIUtil.class, PeriodicSQLHDFSSourceParamHelper.class})
public class ModelSourceBuilderFactoryTest {

    private ModelSourceBuilderFactory factory;
    private Map<String, Object> sourceNodes;
    private Map<String, Object> nodeInfo;
    private Map<String, Object> nodeInputInfo;

    private static void mockRTInfo() {
        try {
            BkFieldConstructor fieldConstructor = PowerMockito.mock(BkFieldConstructor.class);
            PowerMockito.whenNew(BkFieldConstructor.class).withNoArguments().thenReturn(fieldConstructor);
            PowerMockito.when(fieldConstructor.getRtField(anyString())).thenReturn(new ArrayList<NodeField>());
            PeriodicSQLHDFSSourceParamHelper helper = PowerMockito.mock(PeriodicSQLHDFSSourceParamHelper.class);
            PowerMockito.whenNew(PeriodicSQLHDFSSourceParamHelper.class).withAnyArguments().thenReturn(helper);
            List<String> paths = new ArrayList<>();
            Tuple3 tuple3 = new Tuple3(paths, -1L, -1L);
            PowerMockito.when(helper.buildPathAndTimeRangeInput(anyString())).thenReturn(tuple3);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void init(String file) {
        try {
            String content = FileUtil.read(file);
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> parameters = objectMapper.readValue(content, HashMap.class);
            Map<String, Map<String, Object>> nodes = (Map<String, Map<String, Object>>) parameters.get("nodes");
            this.sourceNodes = nodes.get("source");
            this.nodeInfo = (Map<String, Object>) sourceNodes.get("591_mock_table_test");
            this.nodeInputInfo = (Map<String, Object>) this.nodeInfo.get("input");
            long scheduleTime = (long) parameters.get("schedule_time");
            this.factory = new ModelSourceBuilderFactory(scheduleTime, "spark_mllib", false, "http://test/");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @PrepareForTest({ModelIceBergQuerySetSourceNodeBuilder.class, ModelDefaultSourceNodeBuilder.class,
            SQLDebugIcebergNodeBuilder.class, PeriodicSQLIcebergSourceBuilder.class,
            SQLDebugHDFSSourceNodeBuilder.class, PeriodicSQLHDFSSourceNodeBuilder.class,
            ModelDefaultSourceNodeBuilder.class, APIUtil.class, PeriodicSQLHDFSSourceParamHelper.class
    })
    public void testGetBuilder() {
        mockRTInfo();
        this.init("/common_node_topology/test_periodic_topology.conf");
        AbstractBuilder builder;
        builder = factory.getBuilder(this.nodeInfo);
        //nondebug + result_table + parquet
        factory.setDebugLabel(0);
        assert builder instanceof PeriodicSQLIcebergSourceBuilder;

        //debug + result_table + parquet
        factory.setDebugLabel(1);
        builder = factory.getBuilder(this.nodeInfo);
        assert builder instanceof SQLDebugHDFSSourceNodeBuilder;

        //nondebug + result_table + iceberg
        factory.setDebugLabel(0);
        this.nodeInputInfo.put("format", "iceberg");
        builder = factory.getBuilder(this.nodeInfo);
        assert builder instanceof PeriodicSQLIcebergSourceBuilder;

        //debug + result_table + iceberg
        factory.setDebugLabel(1);
        this.nodeInputInfo.put("format", "iceberg");
        builder = factory.getBuilder(this.nodeInfo);
        assert builder instanceof SQLDebugIcebergNodeBuilder;

        // query_set + parquet
        this.nodeInputInfo.put("table_type", "query_set");
        this.nodeInputInfo.put("format", "parquet");
        builder = factory.getBuilder(this.nodeInfo);
        assert builder instanceof ModelDefaultSourceNodeBuilder;

        // query_set + iceberg
        this.nodeInputInfo.put("table_type", "query_set");
        this.nodeInputInfo.put("format", "iceberg");
        builder = factory.getBuilder(this.nodeInfo);
        assert builder instanceof ModelIceBergQuerySetSourceNodeBuilder;

        this.nodeInfo.put("type", "model");
        builder = factory.getBuilder(this.nodeInfo);
        assert builder instanceof ModelDefaultSourceNodeBuilder;
    }

    private Map<String, Object> getIcebergConf() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> hdfsConfig = new HashMap<>();
        hdfsConfig.put("hive.metastore.uris", "test.uris");
        map.put("physical_table_name", "test_table");
        map.put("hdfs_config", hdfsConfig);
        return map;
    }

}
