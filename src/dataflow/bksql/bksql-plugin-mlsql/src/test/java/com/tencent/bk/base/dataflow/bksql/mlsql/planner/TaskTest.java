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

package com.tencent.bk.base.dataflow.bksql.mlsql.planner;

import static org.mockito.ArgumentMatchers.anyString;

import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskContext;
import com.tencent.bk.base.dataflow.bksql.util.MLSqlModelUtils;
import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlDeParser;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModel;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModelStorage;
import com.tencent.bk.base.dataflow.bksql.mlsql.schema.MLSqlSchema;
import com.tencent.bk.base.datalab.bksql.table.BlueKingTrtTableMetadataConnector;
import com.tencent.bk.base.datalab.bksql.table.ColumnMetadata;
import com.tencent.bk.base.datalab.bksql.table.ColumnMetadataImpl;
import com.tencent.bk.base.datalab.bksql.table.MapBasedTableMetadata;
import com.tencent.bk.base.datalab.bksql.table.TableMetadata;
import com.tencent.bk.base.datalab.bksql.util.BlueKingDataTypeMapper;
import com.tencent.bk.base.datalab.bksql.util.DataType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Planner;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.skyscreamer.jsonassert.JSONAssert;

@RunWith(PowerMockRunner.class)
public class TaskTest {

    public static final String METADATA_URL = "http://your.table.meta.serverice.com/";
    public static final String MODEL_METADATA_URL = "http://your.model.meta.service.com/";
    public static final String ERP_URL = "http://your.algrithm.meta.service.com/";

    {
        mockCachedTable();
        mockMLSqlModel();
    }

    private static void mockCachedTable() {
        BlueKingTrtTableMetadataConnector mockMetadataConnector = PowerMockito
                .mock(BlueKingTrtTableMetadataConnector.class);
        PowerMockito.when(mockMetadataConnector.fetchTableMetadata(anyString())).thenAnswer(
                new Answer<TableMetadata<ColumnMetadata>>() {
                    @Override
                    public TableMetadata<ColumnMetadata> answer(InvocationOnMock invocationOnMock) throws Throwable {
                        BlueKingDataTypeMapper mapper = new BlueKingDataTypeMapper();
                        Map<String, ColumnMetadata> map = new HashMap();
                        DataType doubleType = mapper.toBKSqlType("double");
                        DataType stringType = mapper.toBKSqlType("string");
                        DataType intType = mapper.toBKSqlType("int");
                        String description = "description";
                        String alias = "alias";
                        ColumnMetadata ma = new ColumnMetadataImpl("a", doubleType, description);
                        ColumnMetadata mb = new ColumnMetadataImpl("b", doubleType, description);
                        ColumnMetadata mc = new ColumnMetadataImpl("double_1", doubleType, description);
                        ColumnMetadata md = new ColumnMetadataImpl("double_2", doubleType, description);
                        ColumnMetadata me = new ColumnMetadataImpl("c", doubleType, description);
                        ColumnMetadata mf = new ColumnMetadataImpl("d", doubleType, description);
                        ColumnMetadata mg = new ColumnMetadataImpl("e", doubleType, description);
                        ColumnMetadata mh = new ColumnMetadataImpl("run_label", doubleType, description);
                        ColumnMetadata mi = new ColumnMetadataImpl("indexed", doubleType, description);
                        ColumnMetadata mj = new ColumnMetadataImpl("char_1", stringType, description);
                        ColumnMetadata mk = new ColumnMetadataImpl("int_2", intType, description);
                        ColumnMetadata ml = new ColumnMetadataImpl("int_1", intType, description);
                        map.put("a", ma);
                        map.put("b", mb);
                        map.put("double_1", mc);
                        map.put("double_2", md);
                        map.put("c", me);
                        map.put("d", mf);
                        map.put("e", mg);
                        map.put("run_label", mh);
                        map.put("indexed_label", mi);
                        map.put("char_1", mj);
                        map.put("int_2", mk);
                        map.put("int_1", ml);
                        return MapBasedTableMetadata.wrap(map);
                    }
                });
        //mock静态方法，任任意URL都返回提前准备好的Connector
        PowerMockito.mockStatic(BlueKingTrtTableMetadataConnector.class);
        PowerMockito.when(BlueKingTrtTableMetadataConnector.forUrl(anyString())).thenReturn(mockMetadataConnector);
    }

    private static void mockMLSqlModel() {

        PowerMockito.mockStatic(MLSqlModelUtils.class);
        PowerMockito.when(MLSqlModelUtils.getMLSqlModel(anyString())).then(new Answer<MLSqlModel>() {
            @Override
            public MLSqlModel answer(InvocationOnMock invocationOnMock) throws Throwable {
                String modelName = invocationOnMock.getArgument(0);
                MLSqlModel model = new MLSqlModel();
                MLSqlModelStorage storage = new MLSqlModelStorage();
                model.setModelStorage(storage);
                if ("591_pca_model_307".equalsIgnoreCase(modelName)) {
                    model.setAlgorithmName("pca");
                    model.setModelName("591_pca_model_307");
                    storage.setPath("hdfs://xxxx/models/591_pca_model_307");
                    return model;
                } else if ("591_rfc_model_307".equalsIgnoreCase(modelName)) {
                    model.setAlgorithmName("random_forest_classifier");
                    model.setModelName("591_rfc_model_307");
                    storage.setPath("hdfs://xxxx/models/591_rfc_model_307");
                } else if ("591_imputer_model_2".equalsIgnoreCase(modelName)) {
                    model.setAlgorithmName("imputer");
                    model.setModelName("591_imputer_model_2");
                    storage.setPath("hdfs://xxxx/models/591_imputer_model_2");
                }
                return model;
            }
        });
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testTrain() throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("train model 591_k_means_result_2")
                .append(" options(algorithm='k_means', features_col=[a,b], k=2, seed=1,")
                .append(" prediction_col=['prediction']) from 591_test_mock_double_1");
        TaskTester pt = new TaskTester(sb.toString());
        StringBuffer result = new StringBuffer();
        result.append("[{\"evaluate_map\":{},\"id\":\"591_k_means_result_2_train\",")
                .append("\"name\":\"591_k_means_result_2_train\",\"table_name\":null,\"type\":\"model\",")
                .append("\"fields\":[],")
                .append("\"interpreter\":{\"features_col\":{\"value\":[\"a\",\"b\"],\"implement\":\"Arrays\"}},")
                .append("\"processor\":{\"name\":\"k_means\",\"type\":\"train\",")
                .append("\"args\":{\"seed\":\"1\",\"k\":\"2\",\"features_col\":\"features_col\",")
                .append("\"prediction_col\":[\"prediction\"]}},")
                .append("\"parents\":[\"591_test_mock_double_1\"],\"description\":\"591_k_means_result_2_train\",")
                .append("\"model_info\":null,\"model_name\":\"591_k_means_result_2\",\"table_need_create\":true,")
                .append("\"write_mode\":\"overwrite\",\"task_type\":\"mlsql_query\"}]");
        pt.registerTable("student")
                .withContext(new MLSqlTaskContext(sb.toString(), "output_table", null))
                .planContains(result.toString())
                .run();
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testTrainWithSelect() throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("train model encoder_model options(")
                .append("input_cols=[a,b],algorithm='one_hot_encoder_estimator'")
                .append(") AS select a,b from 591_mock_float_1");
        TaskTester pt = new TaskTester(sb.toString());
        StringBuffer result = new StringBuffer();
        result.append("[{")
                .append("\"evaluate_map\":{},")
                .append("\"id\":\"encoder_model_train\",")
                .append("\"name\":\"encoder_model_train\",")
                .append("\"table_name\":null,")
                .append("\"type\":\"model\",")
                .append("\"fields\":[],")
                .append("\"interpreter\":{},")
                .append("\"processor\":{")
                .append("\"name\":\"one_hot_encoder_estimator\",")
                .append("\"type\":\"train\",")
                .append("\"args\":{\"input_cols\":[\"a\",\"b\"]}},")
                .append("\"parents\":[\"591_mock_float_1\"],")
                .append("\"description\":\"encoder_model_train\",")
                .append("\"model_info\":null,")
                .append("\"model_name\":\"encoder_model\",")
                .append("\"table_need_create\":true,")
                .append("\"write_mode\":\"overwrite\",")
                .append("\"task_type\":\"mlsql_query\"")
                .append("}]");
        pt.registerTable("student")
                .withContext(new MLSqlTaskContext(sb.toString(), "output_table", null))
                .planContains(result.toString())
                .run();
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testInsert() throws Exception {
        StringBuffer sb = new StringBuffer();
        sb.append("insert overwrite 591_pca_predict_result(run_label) ")
                .append("run 591_pca_model_307(")
                .append("input_col=[a, b, c, d, e]) as run_label ")
                .append("from 591_mock_float_1");
        StringBuffer result = new StringBuffer();
        result.append("[{")
                .append("\"evaluate_map\":{\"predict_label\":\"run_label\", \"evaluate_type\":\"transform\"},")
                .append("\"id\":\"591_pca_model_307_train_run\",")
                .append("\"name\":\"591_pca_model_307_train_run\",")
                .append("\"table_name\":\"591_pca_predict_result\",")
                .append("\"type\":\"data\",")
                .append("\"fields\":[")
                .append("{\"field\":\"run_label\",\"type\":\"string\",\"description\":null,\"origins\":[\"\"]}")
                .append("],")
                .append("\"interpreter\":{")
                .append("\"input_col\":{\"value\":[\"a\",\"b\",\"c\",\"d\",\"e\"],\"implement\":\"Arrays\"}")
                .append("},")
                .append("\"processor\":{")
                .append("\"name\":\"pca\",")
                .append("\"type\":\"trained-run\",")
                .append("\"args\":{\"input_col\":\"input_col\",\"output_col\":\"run_label\"}")
                .append("},")
                .append("\"parents\":[\"591_mock_float_1\"],")
                .append("\"description\":\"591_pca_model_307_train\",")
                .append("\"model_info\":{")
                .append("\"model_name\":\"591_pca_model_307\",")
                .append("\"model_alias\":\"591_pca_model_307\",")
                .append("\"project_id\":null,")
                .append("\"algorithm_name\":\"pca\",")
                .append("\"model_storage\":{\"path\":\"hdfs://xxxx/models/591_pca_model_307\"},")
                .append("\"train_mode\":null,")
                .append("\"sensitivity\":null,")
                .append("\"active\":null,")
                .append("\"status\":null,")
                .append("\"framework\":null,")
                .append("\"type\":null,")
                .append("\"modelStorageId\":{\"path\":\"hdfs://xxxx/models/591_pca_model_307\"},")
                .append("\"input_config\":null,")
                .append("\"output_config\":null},")
                .append("\"model_name\":null,")
                .append("\"table_need_create\":false,")
                .append("\"write_mode\":\"overwrite\",")
                .append("\"task_type\":\"mlsql_query\"")
                .append("}]");

        TaskTester pt = new TaskTester(sb.toString());
        pt.registerTable("student")
                .withContext(new MLSqlTaskContext(sb.toString(), "output_table", null))
                .planContains(result.toString())
                .run();
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testCreate() throws Exception {
        String sql = "create table 591_pca_predict_result_13 as run 591_pca_model_307("
                + "input_col=[a, b, c, d, e]) "
                + "as run_label,a,b,c,d,e from 591_test_mock_double_1";
        String result = "[{"
                + "\"evaluate_map\":{\"predict_label\":\"run_label\", \"evaluate_type\":\"transform\"},"
                + "\"id\":\"591_pca_model_307_run\","
                + "\"name\":\"591_pca_model_307_run\","
                + "\"table_name\":\"591_pca_predict_result_13\","
                + "\"type\":\"data\","
                + "\"fields\":["
                + "{\"field\":\"run_label\",\"type\":\"string\",\"description\":null,\"origins\":[\"\"]},"
                + "{\"field\":\"a\",\"type\":\"double\",\"description\":null,\"origins\":[\"a\"]},"
                + "{\"field\":\"b\",\"type\":\"double\",\"description\":null,\"origins\":[\"b\"]},"
                + "{\"field\":\"c\",\"type\":\"double\",\"description\":null,\"origins\":[\"c\"]},"
                + "{\"field\":\"d\",\"type\":\"double\",\"description\":null,\"origins\":[\"d\"]},"
                + "{\"field\":\"e\",\"type\":\"double\",\"description\":null,\"origins\":[\"e\"]}],"
                + "\"interpreter\":{"
                + "\"input_col\":{\"value\":[\"a\",\"b\",\"c\",\"d\",\"e\"],"
                + "\"implement\":\"Arrays\"}},"
                + "\"processor\":{"
                + "\"name\":\"pca\","
                + "\"type\":\"trained-run\","
                + "\"args\":{\"input_col\":\"input_col\",\"output_col\":\"run_label\"}},"
                + "\"parents\":[\"591_test_mock_double_1\"],"
                + "\"description\":\"591_pca_model_307_run\","
                + "\"model_info\":{"
                + "\"model_name\":\"591_pca_model_307\","
                + "\"model_alias\":\"591_pca_model_307\","
                + "\"project_id\":null,"
                + "\"algorithm_name\":\"pca\","
                + "\"model_storage\":{\"path\":\"hdfs://xxxx/models/591_pca_model_307\"},"
                + "\"train_mode\":null,"
                + "\"sensitivity\":null,"
                + "\"active\":null,"
                + "\"status\":null,"
                + "\"framework\":null,"
                + "\"type\":null,"
                + "\"modelStorageId\":{\"path\":\"hdfs://xxxx/models/591_pca_model_307\"},"
                + "\"input_config\":null,"
                + "\"output_config\":null},"
                + "\"model_name\":null,"
                + "\"table_need_create\":true,"
                + "\"write_mode\":\"overwrite\","
                + "\"task_type\":\"mlsql_query\"}]";
        TaskTester pt = new TaskTester(sql);
        pt.registerTable("591_mock_float_1")
                .withContext(new MLSqlTaskContext(sql, "output_table", null))
                .planContains(result)
                .run();
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testCreateWithSelect() throws Exception {
        String sql = "create table 591_pca_predict_result_13 "
                + "as run 591_pca_model_307("
                + "input_col=[a, b, c, d, e]) as run_label,a,b,c,d,e "
                + "from (select a,b,c,d,e from 591_test_mock_double_1)";
        String result = "[{"
                + "\"evaluate_map\":{\"predict_label\":\"run_label\", \"evaluate_type\":\"transform\"},"
                + "\"id\":\"591_pca_model_307_run\","
                + "\"name\":\"591_pca_model_307_run\","
                + "\"table_name\":\"591_pca_predict_result_13\","
                + "\"type\":\"data\","
                + "\"fields\":["
                + "{\"field\":\"run_label\",\"type\":\"string\",\"description\":null,\"origins\":[\"\"]},"
                + "{\"field\":\"a\",\"type\":\"double\",\"description\":null,\"origins\":[\"a\"]},"
                + "{\"field\":\"b\",\"type\":\"double\",\"description\":null,\"origins\":[\"b\"]},"
                + "{\"field\":\"c\",\"type\":\"double\",\"description\":null,\"origins\":[\"c\"]},"
                + "{\"field\":\"d\",\"type\":\"double\",\"description\":null,\"origins\":[\"d\"]},"
                + "{\"field\":\"e\",\"type\":\"double\",\"description\":null,\"origins\":[\"e\"]}],"
                + "\"interpreter\":{\"input_col\":{\"value\":[\"a\",\"b\",\"c\",\"d\",\"e\"],"
                + "\"implement\":\"Arrays\"}},"
                + "\"processor\":{\"name\":\"pca\",\"type\":\"trained-run\","
                + "\"args\":{\"input_col\":\"input_col\",\"output_col\":\"run_label\"}},"
                + "\"parents\":[\"591_test_mock_double_1\"],"
                + "\"description\":\"591_pca_model_307_run\","
                + "\"model_info\":{"
                + "\"model_name\":\"591_pca_model_307\","
                + "\"model_alias\":\"591_pca_model_307\","
                + "\"project_id\":null,"
                + "\"algorithm_name\":\"pca\","
                + "\"model_storage\":{\"path\":\"hdfs://xxxx/models/591_pca_model_307\"},"
                + "\"train_mode\":null,"
                + "\"sensitivity\":null,"
                + "\"active\":null,"
                + "\"status\":null,"
                + "\"framework\":null,"
                + "\"type\":null,"
                + "\"modelStorageId\":{\"path\":\"hdfs://xxxx/models/591_pca_model_307\"},"
                + "\"input_config\":null,"
                + "\"output_config\":null},"
                + "\"model_name\":null,"
                + "\"table_need_create\":true,"
                + "\"write_mode\":\"overwrite\","
                + "\"task_type\":\"mlsql_query\"}]";

        TaskTester pt = new TaskTester(sql);
        pt.registerTable("student")
                .withContext(new MLSqlTaskContext(sql, "output_table", null))
                .planContains(result)
                .run();
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testCreateWithLateral() throws Exception {
        String sql = "/**comment*/"
                + "create table 591_imputer_result_4 "
                + "as run int_1,column_a,column_b "
                + "from 591_test_mock_mix_1, lateral table(591_imputer_model_2(input_cols=[double_1,double_2])) "
                + "as T(column_a, column_b)";
        String result = "[{"
                + "\"evaluate_map\":{}, "
                + "\"id\":\"591_imputer_model_2_run_run\","
                + "\"name\":\"591_imputer_model_2_run_run\","
                + "\"table_name\":\"591_imputer_result_4\","
                + "\"type\":\"data\","
                + "\"fields\":["
                + "{\"field\":\"column_a\",\"type\":\"double\",\"description\":null,\"origins\":[\"column_0\"]},"
                + "{\"field\":\"column_b\",\"type\":\"double\",\"description\":null,\"origins\":[\"column_1\"]},"
                + "{\"field\":\"int_1\",\"type\":\"int\",\"description\":null,\"origins\":[\"int_1\"]}],"
                + "\"interpreter\":{"
                + "\"input_cols\":{\"value\":[\"double_1\",\"double_2\"],\"implement\":\"Arrays\"}},"
                + "\"processor\":{\"name\":\"imputer\",\"type\":\"trained-run\","
                + "\"args\":{\"input_cols\":\"input_cols\","
                + "\"lateral_function_output\":\"["
                + "{\\\"name\\\":\\\"output_cols\\\",\\\"type\\\":\\\"DOUBLE\\\"},"
                + "{\\\"name\\\":\\\"output_cols_1\\\",\\\"type\\\":\\\"DOUBLE\\\"}]\","
                + "\"output_cols\":\"column_a\",\"output_cols_1\":\"column_b\"}},"
                + "\"parents\":[\"591_test_mock_mix_1\"],"
                + "\"description\":\"591_imputer_model_2_run_run\","
                + "\"model_info\":"
                + "{\"model_name\":\"591_imputer_model_2\","
                + "\"model_alias\":\"591_imputer_model_2\","
                + "\"project_id\":null,"
                + "\"algorithm_name\":\"imputer\","
                + "\"model_storage\":{\"path\":\"hdfs://xxxx/models/591_imputer_model_2\"},"
                + "\"train_mode\":null,"
                + "\"sensitivity\":null,"
                + "\"active\":null,"
                + "\"status\":null,"
                + "\"framework\":null,"
                + "\"type\":null,"
                + "\"modelStorageId\":{\"path\":\"hdfs://xxxx/models/591_imputer_model_2\"},"
                + "\"input_config\":null,"
                + "\"output_config\":null},"
                + "\"model_name\":null,"
                + "\"table_need_create\":true,"
                + "\"write_mode\":\"overwrite\","
                + "\"task_type\":\"mlsql_query\"}]";

        TaskTester pt = new TaskTester(sql);
        pt.registerTable("student")
                .withContext(new MLSqlTaskContext(sql, "output_table", null))
                .planContains(result)
                .run();
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testDrop() throws Exception {
        String sql = "drop model if exists 591_abc";
        String result = "{"
                + "\"result\":true,"
                + "\"content\":{\"type\":\"drop_model\",\"data\":\"591_abc\",\"ifExists\":true},"
                + "\"code\":\"1586200\"}";
        TaskTester pt = new TaskTester(sql);
        pt.registerTable("student")
                .withContext(new MLSqlTaskContext(sql, "output_table", null))
                .planContains(result)
                .run();

    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testMultiInput() {
        String sql = "create table 591_imputer_result_4 "
                + "as run column_b,column_a from 591_test_mock_mix_1, "
                + "lateral table(imputer_model_2(input_cols=[double_1,double_2,<double_1, double_2>,"
                + "[double_1,double_2],<double_1, double_2>,[double_1,double_2]])) as T(column_b, column_a)";
        String result = "[{"
                + "\"id\":\"imputer_model_2_run_run\","
                + "\"name\":\"imputer_model_2_run_run\","
                + "\"table_name\":\"591_imputer_result_4\","
                + "\"type\":\"data\","
                + "\"fields\":["
                + "{\"field\":\"column_a\",\"type\":\"double\",\"description\":null,\"origins\":[\"column_1\"]},"
                + "{\"field\":\"column_b\",\"type\":\"double\",\"description\":null,\"origins\":[\"column_0\"]}],\""
                + "interpreter\":{"
                + "\"input_cols\":{\"value\":"
                + "[\"double_1\",\"double_2\",\"input_cols_2\",\"input_cols_3\",\"input_cols_4\",\"input_cols_5\"],"
                + "\"implement\":\"Arrays\"},"
                + "\"input_cols_2\":{\"value\":[\"double_1\",\"double_2\"],\"implement\":\"Vectors\"},"
                + "\"input_cols_3\":{\"value\":[\"double_1\",\"double_2\"],\"implement\":\"Arrays\"},"
                + "\"input_cols_4\":{\"value\":[\"double_1\",\"double_2\"],\"implement\":\"Vectors\"},"
                + "\"input_cols_5\":{\"value\":[\"double_1\",\"double_2\"],\"implement\":\"Arrays\"}},"
                + "\"processor\":{\"name\":\"imputer\",\"type\":\"trained-run\","
                + "\"args\":{\"input_cols\":\"input_cols\","
                + "\"lateral_function_output\":\"["
                + "{\\\"name\\\":\\\"output_cols\\\",\\\"type\\\":\\\"DOUBLE\\\"},"
                + "{\\\"name\\\":\\\"output_cols_1\\\",\\\"type\\\":\\\"DOUBLE\\\"}]\","
                + "\"output_cols_1\":\"column_a\",\"output_cols\":\"column_b\"}},"
                + "\"parents\":[\"591_test_mock_mix_1\"],"
                + "\"description\":\"imputer_model_2_run_run\","
                + "\"model_info\":{"
                + "\"model_name\":\"imputer_model_2\","
                + "\"model_alias\":\"imputer_model_2\","
                + "\"project_id\":null,"
                + "\"algorithm_name\":\"imputer\","
                + "\"model_storage\":{\"path\":\"hdfs://xxxx/models/imputer_model_2\"},"
                + "\"train_mode\":null,"
                + "\"sensitivity\":null,"
                + "\"active\":null,"
                + "\"status\":null,"
                + "\"framework\":null,"
                + "\"type\":null,"
                + "\"modelStorageId\":{\"path\":\"hdfs://xxxx/models/imputer_model_2\"},"
                + "\"input_config\":null,"
                + "\"output_config\":null},"
                + "\"model_name\":null,"
                + "\"table_need_create\":true,"
                + "\"write_mode\":\"overwrite\","
                + "\"task_type\":\"mlsql_query\", "
                + "\"evaluate_map\":{}}]";

        TaskTester pt = new TaskTester(sql);
        try {
            pt.registerTable("student")
                    .withContext(new MLSqlTaskContext(sql, "output_table", null))
                    .planContains(result)
                    .run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testTrainWithSubQuery() {
        String sql = "train model 591_string_indexer_276 "
                + "options(algorithm='string_indexer',input_col=char_1,output_col='indexed')  "
                + "from (select double_1, double_2,char_1,power(int_2,3) as pow_int_2 "
                + "from 591_new_modeling_query_set)";
        String result = "["
                + "{\"fields\":[],"
                + "\"parents\":[\"591_new_modeling_query_set\"],"
                + "\"interpreter\":{},"
                + "\"processor\":{\"name\":\"string_indexer\",\"type\":\"train\","
                + "\"args\":{\"output_col\":\"indexed\",\"input_col\":\"char_1\"}},"
                + "\"id\":\"591_string_indexer_276_xxxxxx\","
                + "\"name\":\"591_string_indexer_276_xxxxxx\","
                + "\"table_name\":null,"
                + "\"type\":\"model\","
                + "\"description\":\"591_string_indexer_276_xxxxxx\","
                + "\"model_info\":null,"
                + "\"model_name\":\"591_string_indexer_276\","
                + "\"table_need_create\":true,"
                + "\"write_mode\":\"overwrite\","
                + "\"task_type\":\"mlsql_query\","
                + "\"evaluate_map\":{}}]";

        TaskTester pt = new TaskTester(sql);
        try {
            pt.registerTable("student")
                    .withContext(new MLSqlTaskContext(sql, "output_table", null))
                    .planContains(result)
                    .run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testRunWithEvaluate() throws Exception {
        String sql = "create table 591_rfc_result_307 "
                + "as run 591_rfc_model_307("
                + "features_col=<double_1,double_2>, "
                + "evaluate_function='pyspark_classify_evaluation') as prediction,"
                + "double_1 as label,double_2,indexed  "
                + "from 591_string_indexer_result_307";
        String result = "[{"
                + "\"table_need_create\":true,"
                + "\"description\":\"591_rfc_model_307_run\","
                + "\"interpreter\":{"
                + "\"features_col\":{\"implement\":\"Vectors\",\"value\":[\"double_1\",\"double_2\"]}},"
                + "\"type\":\"data\","
                + "\"processor\":{\"args\":{\"features_col\":\"features_col\",\"prediction_col\":\"prediction\"},"
                + "\"name\":\"random_forest_classifier\","
                + "\"type\":\"trained-run\"},"
                + "\"table_name\":\"591_rfc_result_307\","
                + "\"write_mode\":\"overwrite\","
                + "\"evaluate_map\":{"
                + "\"predict_label\":\"prediction\","
                + "\"evaluate_function\":\"pyspark_classify_evaluation\","
                + "\"evaluate_type\":\"classify\"},"
                + "\"model_name\":null,"
                + "\"model_info\":{"
                + "\"output_config\":null,"
                + "\"active\":null,"
                + "\"model_alias\":\"591_rfc_model_307\","
                + "\"algorithm_name\":\"random_forest_classifier\","
                + "\"type\":null,"
                + "\"framework\":null,"
                + "\"model_name\":\"591_rfc_model_307\","
                + "\"project_id\":null,"
                + "\"input_config\":null,"
                + "\"modelStorageId\":{\"path\":\"hdfs://xxxx/models/591_rfc_model_307\"},"
                + "\"model_storage\":{\"path\":\"hdfs://xxxx/models/591_rfc_model_307\"},"
                + "\"sensitivity\":null,"
                + "\"train_mode\":null,"
                + "\"status\":null},"
                + "\"name\":\"591_rfc_model_307_run\","
                + "\"id\":\"591_rfc_model_307_run\","
                + "\"fields\":["
                + "{\"field\":\"prediction\",\"description\":null,\"origins\":[\"\"],\"type\":\"double\"},"
                + "{\"field\":\"label\",\"description\":null,\"origins\":[\"double_1\"],\"type\":\"double\"},"
                + "{\"field\":\"double_2\",\"description\":null,\"origins\":[\"double_2\"],\"type\":\"double\"},"
                + "{\"field\":\"indexed\",\"description\":null,\"origins\":[\"indexed\"],\"type\":\"double\"}],"
                + "\"task_type\":\"mlsql_query\",\"parents\":[\"591_string_indexer_result_307\"]}]";

        TaskTester pt = new TaskTester(sql);
        pt.registerTable("student")
                .withContext(new MLSqlTaskContext(sql, "output_table", null))
                .planContains(result)
                .run();
    }

    @Test
    @PrepareForTest({BlueKingTrtTableMetadataConnector.class, MLSqlModelUtils.class})
    public void testRunWithEvaluateWithDefault() throws Exception {
        String sql = "create table 591_rfc_result_307 "
                + "as run 591_rfc_model_307(features_col=<double_1,double_2>) as prediction,"
                + "double_1 as label,double_2,indexed  "
                + "from 591_string_indexer_result_307";
        String result = "["
                + "{\"table_need_create\":true,"
                + "\"description\":\"591_rfc_model_307_run\","
                + "\"interpreter\":"
                + "{\"features_col\":{\"implement\":\"Vectors\",\"value\":[\"double_1\",\"double_2\"]}},"
                + "\"type\":\"data\","
                + "\"processor\":{\"args\":{\"features_col\":\"features_col\","
                + "\"prediction_col\":\"prediction\"},"
                + "\"name\":\"random_forest_classifier\","
                + "\"type\":\"trained-run\"},"
                + "\"table_name\":\"591_rfc_result_307\","
                + "\"write_mode\":\"overwrite\","
                + "\"evaluate_map\":{\"predict_label\":\"prediction\",\"evaluate_type\":\"classify\"},"
                + "\"model_name\":null,"
                + "\"model_info\":{"
                + "\"output_config\":null,"
                + "\"active\":null,"
                + "\"model_alias\":\"591_rfc_model_307\","
                + "\"algorithm_name\":\"random_forest_classifier\","
                + "\"type\":null,"
                + "\"framework\":null,"
                + "\"model_name\":\"591_rfc_model_307\","
                + "\"project_id\":null,"
                + "\"input_config\":null,"
                + "\"modelStorageId\":{\"path\":\"hdfs://xxxx/models/591_rfc_model_307\"},"
                + "\"model_storage\":{\"path\":\"hdfs://xxxx/models/591_rfc_model_307\"},"
                + "\"sensitivity\":null,"
                + "\"train_mode\":null,"
                + "\"status\":null},"
                + "\"name\":\"591_rfc_model_307_run\","
                + "\"id\":\"591_rfc_model_307_run\","
                + "\"fields\":["
                + "{\"field\":\"prediction\",\"description\":null,\"origins\":[\"\"],\"type\":\"double\"},"
                + "{\"field\":\"label\",\"description\":null,\"origins\":[\"double_1\"],\"type\":\"double\"},"
                + "{\"field\":\"double_2\",\"description\":null,\"origins\":[\"double_2\"],\"type\":\"double\"},"
                + "{\"field\":\"indexed\",\"description\":null,\"origins\":[\"indexed\"],\"type\":\"double\"}],"
                + "\"task_type\":\"mlsql_query\",\"parents\":[\"591_string_indexer_result_307\"]}]";

        TaskTester pt = new TaskTester(sql);
        pt.registerTable("student")
                .withContext(new MLSqlTaskContext(sql, "output_table", null))
                .planContains(result)
                .run();
    }


    public TaskTester sql(String sql) {
        return new TaskTester(sql);
    }

    private static class TaskTester {

        private final String sql;
        private String expected = null;
        private Set<String> tables = new HashSet<>();
        //private MLSqlTaskContext context;

        private TaskTester(String sql) {
            this.sql = sql;
        }

        private TaskTester planContains(String expected) {
            this.expected = expected;
            return this;
        }

        private TaskTester registerTable(String tableName) {
            tables.add(tableName);
            return this;
        }

        private TaskTester withContext(MLSqlTaskContext context) {
            //this.context = context;
            return this;
        }

        private void run() throws Exception {
            MLSqlSchema schema = new MLSqlSchema();
            Planner planner = MLSqlPlannerFactory.get()
                    .withDefaultSchema(schema);
            SqlNode sqlNode = planner.parse(sql);
            MLSqlDeParser deParser = new MLSqlDeParser(METADATA_URL, MODEL_METADATA_URL, ERP_URL);
            String result = deParser.deParse(sqlNode).toString();
            String newexpected = this.expected
                    .replaceAll("591\\_string\\_indexer\\_276\\_([0-9]|[a-z])+", "591_string_indexer_276_xxxxxx");
            String newresult = result
                    .replaceAll("591\\_string\\_indexer\\_276\\_([0-9]|[a-z])+", "591_string_indexer_276_xxxxxx");

            System.out.println("expected:" + newexpected);
            System.out.println("neresult:" + newresult);
            if (newexpected.startsWith("[")) {
                JSONArray expectedResult = new JSONArray(newexpected);
                JSONArray neResult = new JSONArray(newresult);
                JSONAssert.assertEquals(expectedResult, neResult, true);
            } else {
                JSONObject expectedResult = new JSONObject(newexpected);
                JSONObject neResult = new JSONObject(newresult);
                JSONAssert.assertEquals(expectedResult, neResult, true);
            }
        }
    }
}
