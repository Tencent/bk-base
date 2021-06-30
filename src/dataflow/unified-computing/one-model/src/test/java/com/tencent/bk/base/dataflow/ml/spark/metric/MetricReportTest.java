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

package com.tencent.bk.base.dataflow.ml.spark.metric;

import static org.mockito.ArgumentMatchers.anyString;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.sink.AbstractSink;
import com.tencent.bk.base.dataflow.core.source.AbstractSource;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SinkNode;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.transform.AbstractTransform;
import com.tencent.bk.base.dataflow.ml.exceptions.DataSourceEmptyModelException;
import com.tencent.bk.base.dataflow.ml.exceptions.TransformTypeModelException;
import com.tencent.bk.base.dataflow.ml.metric.DebugMetric;
import com.tencent.bk.base.dataflow.ml.metric.DebugResultData;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSinkNode;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSinkNode.ModelDefaultSinkNodeBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSourceNode.ModelDefaultSourceNodeBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelIceBergQuerySetSinkNode.ModelIceBergQuerySetSinkNodeBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelIceBergQuerySetSourceNode.ModelIceBergQuerySetSourceNodeBuilder;
import com.tencent.bk.base.dataflow.ml.node.ModelSourceBuilderFactory;
import com.tencent.bk.base.dataflow.ml.node.ModelTransformNode;
import com.tencent.bk.base.dataflow.ml.spark.sink.MLDefaultSink;
import com.tencent.bk.base.dataflow.ml.spark.sink.MLIcebergQuerySetSink;
import com.tencent.bk.base.dataflow.ml.spark.sink.MLQuerySetSink;
import com.tencent.bk.base.dataflow.ml.spark.sink.MLSinkStage;
import com.tencent.bk.base.dataflow.ml.spark.source.MLDefaultSource;
import com.tencent.bk.base.dataflow.ml.spark.source.MLIcebergQuerySetSource;
import com.tencent.bk.base.dataflow.ml.spark.source.MLQuerySetSource;
import com.tencent.bk.base.dataflow.ml.spark.source.MLSourceStage;
import com.tencent.bk.base.dataflow.ml.spark.transform.MLTransformStage;
import com.tencent.bk.base.dataflow.ml.spark.transform.SparkMLTrainTransform;
import com.tencent.bk.base.dataflow.ml.spark.transform.SparkMLTrainedRunTransform;
import com.tencent.bk.base.dataflow.ml.spark.transform.SparkMLUntrainedRunTransform;
import com.tencent.bk.base.dataflow.ml.spark.utils.DataUtil;
import com.tencent.bk.base.dataflow.ml.spark.utils.FileUtil;
import com.tencent.bk.base.dataflow.ml.topology.ModelTopology;
import com.tencent.bk.base.dataflow.ml.transform.AbstractMLTransform;
import com.tencent.bk.base.dataflow.spark.pipeline.sink.BatchHDFSSinkOperator;
import com.tencent.bk.base.dataflow.spark.pipeline.sink.BatchIcebergSinkOperator;
import com.tencent.bk.base.dataflow.spark.pipeline.source.BatchHDFSSourceOperator;
import com.tencent.bk.base.dataflow.spark.pipeline.source.BatchIcebergSourceOperator;
import com.tencent.bk.base.dataflow.spark.pipeline.transform.BatchSQLTransformOperator;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLHDFSSinkNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLHDFSSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLHDFSSourceParamHelper;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLIcebergSinkBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic.PeriodicSQLIcebergSourceBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug.SQLDebugHDFSSourceNodeBuilder;
import com.tencent.bk.base.dataflow.spark.sql.topology.builder.sqldebug.SQLDebugIcebergNodeBuilder;
import com.tencent.bk.base.dataflow.spark.topology.BkFieldConstructor;
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper;
import com.tencent.bk.base.dataflow.spark.topology.HDFSDataFrameWrapper;
import com.tencent.bk.base.dataflow.spark.topology.HDFSPathConstructor;
import com.tencent.bk.base.dataflow.metrics.BaseMetric;
import com.tencent.bk.base.dataflow.metrics.Literal;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.Tuple3;
import scala.collection.JavaConverters;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({BaseMetric.class, Literal.class, HDFSPathConstructor.class, DebugResultData.class,
        PeriodicSQLHDFSSinkNodeBuilder.class, BkFieldConstructor.class, StringIndexerModel.class,
        ModelDefaultSinkNodeBuilder.class, PeriodicSQLIcebergSinkBuilder.class,
        ModelIceBergQuerySetSinkNodeBuilder.class, ModelIceBergQuerySetSourceNodeBuilder.class,
        ModelDefaultSourceNodeBuilder.class, ModelSourceBuilderFactory.class, Literal.class,
        SQLDebugIcebergNodeBuilder.class, PeriodicSQLIcebergSourceBuilder.class, DebugMetric.class,
        SQLDebugHDFSSourceNodeBuilder.class, PeriodicSQLHDFSSourceNodeBuilder.class,
        ModelDefaultSourceNodeBuilder.class, PeriodicSQLHDFSSourceParamHelper.class
})
public class MetricReportTest {

    private static SparkSession spark;
    private ModelDefaultSinkNode node;
    private Dataset<Row> dataset;
    private ModelTopology topology;
    private SinkNode sinkNode;

    private static void mockMetrics() {
        try {
            BaseMetric baseMetric = PowerMockito.mock(BaseMetric.class);
            PowerMockito.whenNew(BaseMetric.class).withAnyArguments().thenReturn(baseMetric);
            PowerMockito.when(baseMetric.getMetricsStringAndReport()).thenReturn(new HashMap<String, String>());
            PowerMockito.when(baseMetric.literal(anyString())).thenReturn(new Literal());

            //MetricRegistry registry = PowerMockito.mock(MetricRegistry.class);
            //PowerMockito.whenNew(MetricRegistry.class).withNoArguments().thenReturn(registry);
            //PowerMockito.when(registry.literal(anyString())).thenReturn(new Literal());

            //DataPlatformMetric dataPlatformMetric = PowerMockito.mock(DataPlatformMetric.class);
            //PowerMockito.whenNew(DataPlatformMetric.class).withAnyArguments().thenReturn(dataPlatformMetric);

            HDFSPathConstructor pathConstructor = PowerMockito.mock(HDFSPathConstructor.class);
            PowerMockito.whenNew(HDFSPathConstructor.class).withNoArguments().thenReturn(pathConstructor);
            PowerMockito.when(pathConstructor.getHdfsRoot(anyString())).thenReturn("hdfs://xxxx/");
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

    @BeforeClass
    public static void setup() {
        System.setProperty("HADOOP_USER_NAME", "test");
        spark = SparkSession
                .builder()
                .appName("JavaTokenizerExample")
                .master("local")
                .getOrCreate();
    }

    @AfterClass
    public static void destroy() {
        if (spark != null) {
            spark.sessionState().catalog().reset();
            spark.stop();
            spark = null;
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    /**
     * 初始化测试环境
     */
    public void init(String file) {
        try {
            String content = FileUtil.read(file);
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> parameters = objectMapper.readValue(content, HashMap.class);
            parameters.put("run_mode", "debug");
            ModelTopology.ModelTopologyBuilder builder = new ModelTopology.ModelTopologyBuilder(parameters);
            this.topology = new ModelTopology(builder);
            sinkNode = this.topology.getSinkNodes().get("591_random_forest_result");

            Map<String, Map<String, Object>> nodes = (Map<String, Map<String, Object>>) parameters.get("nodes");
            Map<String, Object> sinkNodes = nodes.get("sink");
            Map<String, Object> sinkNodeInfo = (Map<String, Object>) sinkNodes.get("591_random_forest_result");
            ModelDefaultSinkNodeBuilder nodeBuilder = new ModelDefaultSinkNodeBuilder(sinkNodeInfo);
            node = new ModelDefaultSinkNode(nodeBuilder);
            dataset = DataUtil.getInputDataset(spark);

        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    @Test
    public void testReportResult() {
        System.setProperty("HADOOP_USER_NAME", "test");
        System.setProperty("SPARK_LOCAL_IP", "127.0.0.1");
        mockMetrics();
        this.init("/common_node_topology/test_periodic_topology.conf");
        DebugResultData result = new DebugResultData(sinkNode, topology);
        Map<String, Object> reportResult = result.reportResult(DataUtil.getInputDataset(spark));
        System.out.println("report result:" + reportResult);
        List<Map<String, String>> reportData = (List<Map<String, String>>) reportResult.get("result_data");
        assert reportData.size() == 3;
        assert Integer.parseInt(reportData.get(0).get("id").toString()) == 0;
        assert "Hi I heard about Spark".equalsIgnoreCase(reportData.get(0).get("sentence"));
        assert reportResult.get("result_table_id").toString().equalsIgnoreCase(sinkNode.getNodeId());
    }

    @Test
    public void testCastFieldExpr() {
        Map<String, DataType> dataSchema = new HashMap<>();
        dataSchema.put("field1", DataTypes.IntegerType);
        dataSchema.put("field2", DataTypes.StringType);
        NodeField field = new NodeField();
        field.setField("field3");
        field.setType("int");
        mockMetrics();
        this.init("/common_node_topology/test_periodic_topology.conf");
        MLQuerySetSink sink = new MLQuerySetSink(node, dataset);
        //not contain exception
        boolean exception = false;
        try {
            sink.castFieldExpr(field, dataSchema);
        } catch (RuntimeException e) {
            exception = true;
        }
        assert exception;

        //not support type
        field.setField("field1");
        dataSchema.put("field1", DataTypes.BinaryType);
        String castReturn = sink.castFieldExpr(field, dataSchema);
        System.out.println("cast return:" + castReturn);
        assert "cast(field1 as STRING) as field1".equals(castReturn);

        //contains but not illegal
        dataSchema.put("field1", DataTypes.IntegerType);
        field.setType("int1");
        exception = false;
        try {
            sink.castFieldExpr(field, dataSchema);
        } catch (TransformTypeModelException e) {
            exception = true;
        }
        assert exception;
        //normal return
        field.setType("int");
        String result = sink.castFieldExpr(field, dataSchema);
        assert "field1".equals(result);
    }

    @Test
    public void testCreateSource() {
        mockMetrics();
        this.init("/common_node_topology/test_multi_input_output_topology.conf");
        Map<String, Model<?>> models = new HashMap<>();
        Map<String, DataFrameWrapper> dataSets = new HashMap<>();
        MLSourceStage stage = new MLSourceStage(models, dataSets, spark);
        //model
        SourceNode sourceNode = this.topology.getSourceNodes().get("591_string_indexer_204");
        AbstractSource source = stage.createSource(sourceNode);
        assert source instanceof MLDefaultSource;

        //queryset parquet
        sourceNode = this.topology.getSourceNodes().get("591_mock_test_result_queryset_parquet");
        source = stage.createSource(sourceNode);
        assert source instanceof MLQuerySetSource;

        //queryset iceberg
        sourceNode = this.topology.getSourceNodes().get("591_mock_test_result_queryset_iceberg");
        source = stage.createSource(sourceNode);
        assert source instanceof MLIcebergQuerySetSource;

        //result_table parquet
        sourceNode = this.topology.getSourceNodes().get("591_mock_test_result_result_table_parquet");
        source = stage.createSource(sourceNode);
        assert source instanceof BatchHDFSSourceOperator;

        //result_table iceberg
        sourceNode = this.topology.getSourceNodes().get("591_mock_test_result_result_table_iceberg");
        source = stage.createSource(sourceNode);
        assert source instanceof BatchIcebergSourceOperator;
    }

    @Test
    public void testCreateSink() {
        mockMetrics();
        this.init("/common_node_topology/test_multi_input_output_topology.conf");
        Map<String, Model<?>> models = new HashMap<>();
        Map<String, DataFrameWrapper> dataSets = new HashMap<>();
        MLSinkStage stage = new MLSinkStage(models, dataSets, spark);
        List<String> locations = new ArrayList<>();
        scala.collection.immutable.List pathList = JavaConverters.asScalaIteratorConverter(locations.iterator())
                .asScala().toList();
        DataFrameWrapper dataFrameWrapper = new HDFSDataFrameWrapper(spark, DataUtil.getInputDataset(spark),
                node.getNodeId(), pathList);
        AbstractSink resultSink = null;
        //model
        SinkNode sinkNode = this.topology.getSinkNodes().get("591_random_forest_result_model");
        resultSink = stage.createSink(sinkNode, null, dataFrameWrapper);
        assert resultSink instanceof MLDefaultSink;
        //queryset + parquet
        sinkNode = this.topology.getSinkNodes().get("591_random_forest_queryset_parquet");
        resultSink = stage.createSink(sinkNode, null, dataFrameWrapper);
        assert resultSink instanceof MLQuerySetSink;
        // iceberg queryset
        sinkNode = this.topology.getSinkNodes().get("591_random_forest_queryset_iceberg");
        resultSink = stage.createSink(sinkNode, null, dataFrameWrapper);
        assert resultSink instanceof MLIcebergQuerySetSink;
        //result_table parquet
        sinkNode = this.topology.getSinkNodes().get("591_random_forest_result");
        resultSink = stage.createSink(sinkNode, null, dataFrameWrapper);
        assert resultSink instanceof BatchHDFSSinkOperator;

        //result_table iceberg
        sinkNode = this.topology.getSinkNodes().get("591_random_forest_result_iceberg");
        if (sinkNode != null) {
            resultSink = stage.createSink(sinkNode, null, null);
            assert resultSink instanceof BatchIcebergSinkOperator;
        }
    }

    @Test
    public void testCreateTransform() {
        mockMetrics();
        this.init("/common_node_topology/test_multi_input_output_topology.conf");
        Map<String, Model<?>> models = new HashMap<>();
        Map<String, DataFrameWrapper> datasets = new HashMap<>();
        MLTransformStage stage = new MLTransformStage(models, datasets, spark);
        TransformNode node = null;
        AbstractTransform transform = null;
        //untrained-node
        node = this.topology.getTransformNodes().get("591_string_indexer_result_204");
        transform = stage.createTransform(node);
        assert transform instanceof SparkMLUntrainedRunTransform;

        //trained-run
        node = this.topology.getTransformNodes().get("591_rfc_result_204");
        transform = stage.createTransform(node);
        assert transform instanceof SparkMLTrainedRunTransform;

        //train
        node = this.topology.getTransformNodes().get("591_rfc_result_204_train");
        transform = stage.createTransform(node);
        assert transform instanceof SparkMLTrainTransform;

        //unknown type
        boolean exception = false;
        try {
            node = this.topology.getTransformNodes().get("591_rfc_result_204_unknown");
            transform = stage.createTransform(node);
        } catch (RuntimeException e) {
            exception = true;
        }
        assert exception;
        //子查询
        node = this.topology.getTransformNodes().get("591_rfc_result_204_sub_query");
        transform = stage.createTransform(node);
        assert transform instanceof BatchSQLTransformOperator;
    }

    @Test
    public void testGetTransformInputDataset() throws Exception {
        mockMetrics();
        this.init("/common_node_topology/test_multi_input_output_topology.conf");
        ModelTransformNode transformNode = (ModelTransformNode) this.topology.getTransformNodes()
                .get("591_string_indexer_result_204");
        Map<String, Model<?>> models = new HashMap<>();
        Map<String, DataFrameWrapper> dataSets = new HashMap<>();

        SparkMLUntrainedRunTransform transform = new SparkMLUntrainedRunTransform(transformNode, models, dataSets,
                spark);
        Method wrapDatasetMethod = AbstractMLTransform.class
                .getDeclaredMethod("wrapDataset", new Class[]{Dataset.class, String.class});
        wrapDatasetMethod.setAccessible(true);
        DataFrameWrapper wrappedDataset = (DataFrameWrapper) wrapDatasetMethod.invoke(transform,
                new Object[]{DataUtil.getEmptyDataset(spark), "591_mock_test_result_queryset_parquet"});
        Method getInputDatasetMethod = AbstractMLTransform.class
                .getDeclaredMethod("getInputDataset", new Class[]{});
        getInputDatasetMethod.setAccessible(true);
        // empty dataset
        dataSets.put("591_mock_test_result_queryset_parquet", wrappedDataset);
        try {
            getInputDatasetMethod.invoke(transform, new Object[]{});
        } catch (Exception e) {
            assert e instanceof DataSourceEmptyModelException;
        }

        //sample dataset
        wrappedDataset = (DataFrameWrapper) wrapDatasetMethod.invoke(transform,
                new Object[]{DataUtil.getInputDataset(spark), "591_mock_test_result_queryset_parquet"});
        dataSets.put("591_mock_test_result_queryset_parquet", wrappedDataset);
        Dataset<Row> inputDataset = (Dataset<Row>) getInputDatasetMethod.invoke(transform, new Object[]{});
        assert inputDataset.count() == 3;
    }

    @Test
    public void testInitProcessorArgs() throws Exception {
        mockMetrics();
        StringIndexerModel model = PowerMockito.mock(StringIndexerModel.class);
        PowerMockito.when(model.labels()).thenReturn(new String[0]);
        this.init("/common_node_topology/test_multi_input_output_topology.conf");
        Map<String, Model<?>> models = new HashMap<>();
        models.put("test_label_model", model);
        Map<String, Object> args = new HashMap<>();
        Map labelMap = new HashMap();
        labelMap.put("test_label_model", model);
        args.put("labels", labelMap);
        Map<String, DataFrameWrapper> dataSets = new HashMap<>();
        ModelTransformNode transformNode = (ModelTransformNode) this.topology.getTransformNodes()
                .get("591_string_indexer_result_204");
        SparkMLUntrainedRunTransform transform = new SparkMLUntrainedRunTransform(transformNode, models, dataSets,
                spark);

        Method initProcessorArgsMethod = AbstractMLTransform.class
                .getDeclaredMethod("initProcessorArgs", new Class[]{Map.class});
        initProcessorArgsMethod.setAccessible(true);
        initProcessorArgsMethod.invoke(transform, new Object[]{args});
        assert args.containsKey("labels");

        String labelSting = args.get("labels").toString();
        assert "[]".equalsIgnoreCase(labelSting);
        //assert list.size() == 0;
    }

    @Test
    public void testInterpreter() throws Exception {
        mockMetrics();
        this.init("/common_node_topology/test_multi_input_output_topology.conf");
        ModelTransformNode transformNode = null;
        Map<String, Model<?>> models = new HashMap<>();
        Map<String, DataFrameWrapper> dataSets = new HashMap<>();
        SparkMLUntrainedRunTransform transform = null;
        Method interpreteMethod = AbstractMLTransform.class
                .getDeclaredMethod("interpreter", new Class[]{Dataset.class});
        interpreteMethod.setAccessible(true);
        //no interpreter
        transformNode = (ModelTransformNode) this.topology.getTransformNodes()
                .get("591_string_indexer_result_204");
        transform = new SparkMLUntrainedRunTransform(transformNode, models, dataSets,
                spark);
        Dataset<Row> dataset = DataUtil.getOutputDataset(spark);
        Dataset<Row> result = (Dataset<Row>) interpreteMethod.invoke(transform, new Object[]{dataset});
        assert result == dataset;

        // interpreter vector
        transformNode = (ModelTransformNode) this.topology.getTransformNodes()
                .get("591_rfc_result_204");
        transform = new SparkMLUntrainedRunTransform(transformNode, models, dataSets,
                spark);
        result = (Dataset<Row>) interpreteMethod.invoke(transform, new Object[]{dataset});
        assert result != dataset;
        StructType schema = result.schema();
        StructField[] fields = schema.fields();
        Map<String, DataType> typeMap = Arrays.asList(fields)
                .stream()
                .map(elem -> new Object[]{elem.name(), elem.dataType()})
                .collect(Collectors.toMap(e -> (String) e[0], e -> (DataType) e[1]));
        assert typeMap.containsKey("id");
        assert typeMap.containsKey("count");
        assert typeMap.containsKey("features_col");
        assert typeMap.get("id") == DataTypes.IntegerType;
        assert typeMap.get("count") == DataTypes.IntegerType;
        assert typeMap.get("features_col") instanceof VectorUDT;

        //interprete array
        transformNode = (ModelTransformNode) this.topology.getTransformNodes()
                .get("591_rfc_result_204_train");
        transform = new SparkMLUntrainedRunTransform(transformNode, models, dataSets,
                spark);
        result = (Dataset<Row>) interpreteMethod.invoke(transform, new Object[]{dataset});
        assert result != dataset;
        schema = result.schema();
        fields = schema.fields();
        typeMap = Arrays.asList(fields)
                .stream()
                .map(elem -> new Object[]{elem.name(), elem.dataType()})
                .collect(Collectors.toMap(e -> (String) e[0], e -> (DataType) e[1]));
        assert typeMap.containsKey("id");
        assert typeMap.containsKey("count");
        assert typeMap.containsKey("features_col");
        assert typeMap.get("id") == DataTypes.IntegerType;
        assert typeMap.get("count") == DataTypes.IntegerType;
        assert typeMap.get("features_col") instanceof ArrayType;

        //unsupported
        boolean exception = false;
        transformNode = (ModelTransformNode) this.topology.getTransformNodes()
                .get("591_rfc_result_204_unknown");
        transform = new SparkMLUntrainedRunTransform(transformNode, models, dataSets,
                spark);
        try {
            interpreteMethod.invoke(transform, new Object[]{dataset});
        } catch (Exception e) {
            exception = true;
        }
        assert exception;
    }

    @Test
    public void testRenameColumn() throws Exception {
        mockMetrics();
        this.init("/common_node_topology/test_multi_input_output_topology.conf");
        ModelTransformNode transformNode = (ModelTransformNode) this.topology.getTransformNodes()
                .get("591_string_indexer_result_204");
        Map<String, Model<?>> models = new HashMap<>();
        Map<String, DataFrameWrapper> dataSets = new HashMap<>();
        SparkMLUntrainedRunTransform transform = new SparkMLUntrainedRunTransform(transformNode, models, dataSets,
                spark);
        Method renameMethod = AbstractMLTransform.class
                .getDeclaredMethod("renameColumn", new Class[]{ModelTransformNode.class, Dataset.class});
        renameMethod.setAccessible(true);
        Dataset<Row> dataset = DataUtil.getOutputDataset(spark);
        Dataset<Row> result = (Dataset<Row>) renameMethod.invoke(transform, new Object[]{transformNode, dataset});
        StructType schema = result.schema();
        StructField[] fields = schema.fields();
        Map<String, DataType> typeMap = Arrays.asList(fields)
                .stream()
                .map(elem -> new Object[]{elem.name(), elem.dataType()})
                .collect(Collectors.toMap(e -> (String) e[0], e -> (DataType) e[1]));
        assert typeMap.containsKey("id1");
        assert typeMap.containsKey("count1");
    }

}
