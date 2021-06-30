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

package com.tencent.bk.base.dataflow.bksql.deparser;

import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserAPIV3Test.METADATA_URL;
import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserAPIV3Test.STATIC_METADATA_URL;
import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserAPIV3Test.UDF_METADATA_URL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.tencent.blueking.bksql.function.udf.BkdataUdfMetadataConnector;
import com.tencent.blueking.bksql.function.udf.UdfArgs;
import com.tencent.blueking.bksql.function.udf.UdfMetadata;
import com.tencent.blueking.bksql.function.udf.UdfParameterMetadata;
import com.tencent.blueking.bksql.table.BlueKingStaticTableMetadataConnector;
import com.tencent.blueking.bksql.table.BlueKingTrtTableMetadataConnector;
import com.tencent.blueking.bksql.table.ColumnMetadata;
import com.tencent.blueking.bksql.table.ColumnMetadataImpl;
import com.tencent.blueking.bksql.table.MapBasedTableMetadata;
import com.tencent.blueking.bksql.table.StaticColumnMetadata;
import com.tencent.blueking.bksql.table.TableMetadata;
import com.tencent.blueking.bksql.util.DataType;
import com.tencent.blueking.bksql.util.UdfType;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.json.JSONArray;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

public class TaskTester {
    private final String sql;
    private String expected = null;
    private String tableConf = null;

    public TaskTester(String sql) {
        this.sql = sql;
    }

    /**
     * 执行sql解析验证结果
     *
     * @param sql sql
     * @param deParserAPIV3 解析类实例
     * @param expected 期望结果
     * @throws JSQLParserException
     * @throws JsonProcessingException
     */
    public static void runTestCase(String sql,
                                   FlinkSqlDeParserAPIV3 deParserAPIV3,
                                   String expected) throws JSQLParserException, JsonProcessingException {
        Statement parsed = CCJSqlParserUtil.parse(sql);
        Object deParsed = deParserAPIV3.deParse(parsed);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        String actual = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(deParsed);
        Assert.assertThat(actual, new BaseMatcher<Object>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("task contains " + expected);
            }

            @Override
            public boolean matches(Object item) {
                final String actual = (String) item;
                return actual.replace("\r\n", "\n").contains(expected);
            }

            @Override
            public void describeMismatch(Object item, Description description) {
                description.appendText("was ").appendText((String) item);
            }
        });
    }

    public TaskTester setTableConf(String tableConf) {
        this.tableConf = tableConf;
        return this;
    }

    public TaskTester taskContains(String expected) throws IOException {
        this.expected = expected;
        return this;
    }

    /**
     * 运行sql解析
     *
     * @throws Exception
     */
    public void run() throws Exception {
        //mock测试udf cache数据
        mockCacheUdfInfo();
        //mock测试static cache数据
        mockCacheStaticTable();
        //mock测试general table cache数据
        mockCacheGenTable();
        FlinkSqlDeParserAPIV3 deParserAPIV3 = new FlinkSqlDeParserAPIV3Mock(
                ConfigFactory.parseString(this.tableConf),
                METADATA_URL,
                STATIC_METADATA_URL,
                UDF_METADATA_URL,
                Arrays.asList("COUNT", "AVG", "SUM", "MAX", "MIN", "BKDATA_LAST", "BKDATA_SUM", "GROUP_CONCAT"));
        runTestCase(sql, deParserAPIV3, expected);
    }

    private void mockCacheUdfInfo() throws Exception {
        Map<UdfArgs, List<UdfMetadata>> entryMap = Maps.newHashMap();
        //填充udf测试数据
        //UDF1 -> udf_java_udtf
        addUdfToMap(entryMap,
                Arrays.asList("string", "string"),
                Arrays.asList("string", "int"),
                "udf_java_udtf",
                UdfType.UDTF,
                "dev");
        //UDF2 -> udf_python_udf
        addUdfToMap(entryMap,
                Arrays.asList("string", "string"),
                Arrays.asList("string"),
                "udf_python_udf",
                UdfType.UDF,
                "dev");
        //UDF3 -> udf_py_udaf2
        addUdfToMap(entryMap,
                Arrays.asList("long"),
                Arrays.asList("string"),
                "udf_py_udaf2",
                UdfType.UDAF,
                "dev");

        //mock 静态方法提前准备udf cache数据
        BkdataUdfMetadataConnector mockUdfConnector = PowerMockito.mock(BkdataUdfMetadataConnector.class);
        PowerMockito.when(mockUdfConnector.fetchUdfMetaData(any())).thenAnswer(new Answer<List<UdfMetadata>>() {
            @Override
            public List<UdfMetadata> answer(InvocationOnMock invocationOnMock) throws Throwable {
                UdfArgs udfArgs = invocationOnMock.getArgument(0);
                String env = udfArgs.getEnv();
                String functionName = udfArgs.getFunctionName();
                for (Entry<UdfArgs, List<UdfMetadata>> entry : entryMap.entrySet()) {
                    if (env.equals(entry.getKey().getEnv())
                            && functionName.equals(entry.getKey().getFunctionName())) {
                        return entry.getValue();
                    }
                }
                return null;
            }
        });

        //mock静态方法，任任意URL都返回提前准备好的Connector
        PowerMockito.mockStatic(BkdataUdfMetadataConnector.class);
        PowerMockito.when(BkdataUdfMetadataConnector.forUrl(anyString())).thenReturn(mockUdfConnector);
    }

    private void addUdfToMap(Map<UdfArgs, List<UdfMetadata>> entryMap,
                             List<String> inputTypes,
                             List<String> outputTypes,
                             String functionName,
                             UdfType udfType,
                             String env) {
        List<UdfParameterMetadata> javaParamMetaList = Lists.newArrayList();
        javaParamMetaList
                .add(new UdfParameterMetadata(inputTypes, outputTypes));
        UdfMetadata javaUdfMetadata = new UdfMetadata(functionName, udfType, javaParamMetaList);
        UdfArgs javaUdfArgs = new UdfArgs(env, functionName);
        List<UdfMetadata> javaUdfMetaList = Lists.newArrayList();
        javaUdfMetaList.add(javaUdfMetadata);
        entryMap.put(javaUdfArgs, javaUdfMetaList);
    }

    private void mockCacheStaticTable() throws Exception {
        Map<String, Map<String, ColumnMetadata>> metaMap = Maps.newHashMap();
        Map<String, Map<String, Object>> storageMap = Maps.newHashMap();
        Map<String, List<String>> keyMap = Maps.newHashMap();
        //填充static table cache测试数据
        initStaticTableIgniteWrite2(metaMap, storageMap, keyMap);
        initStaticIgniteWriteRedis2Node(metaMap, storageMap, keyMap);
        initStaticTableIplib(metaMap, storageMap, keyMap);
        initStaticTablef1630(metaMap, storageMap, keyMap);
        //mock 静态方法提前准备udf cache数据
        BlueKingStaticTableMetadataConnector mockStaticTableConnector = PowerMockito
                .mock(BlueKingStaticTableMetadataConnector.class);
        // mock 获取meta方法
        PowerMockito.when(mockStaticTableConnector.fetchTableMetadata(any()))
                .thenAnswer(new Answer<TableMetadata<ColumnMetadata>>() {
                    @Override
                    public TableMetadata<ColumnMetadata> answer(InvocationOnMock invocationOnMock)
                            throws Throwable {
                        String tableName = invocationOnMock.getArgument(0);
                        return MapBasedTableMetadata.wrap(metaMap.get(tableName));
                    }
                });
        // mock 获取storage方法
        PowerMockito.when(mockStaticTableConnector.fetchStorageInfo(any()))
                .thenAnswer(new Answer<Map<String, Object>>() {
                    @Override
                    public Map<String, Object> answer(InvocationOnMock invocationOnMock)
                            throws Throwable {
                        String tableName = invocationOnMock.getArgument(0);
                        return storageMap.get(tableName);
                    }
                });
        // mock 获取keys方法
        PowerMockito.when(mockStaticTableConnector.fetchKeys(any()))
                .thenAnswer(new Answer<List<String>>() {
                    @Override
                    public List<String> answer(InvocationOnMock invocationOnMock)
                            throws Throwable {
                        String tableName = invocationOnMock.getArgument(0);
                        return keyMap.get(tableName);
                    }
                });
        //mock bizID
        PowerMockito.when(mockStaticTableConnector.fetchBizID(any())).thenReturn("591");

        //mock静态方法，任任意URL都返回提前准备好的Connector
        PowerMockito.mockStatic(BlueKingStaticTableMetadataConnector.class);
        PowerMockito.when(BlueKingStaticTableMetadataConnector.forUrl(anyString()))
                .thenReturn(mockStaticTableConnector);
    }

    private void initStaticTable(Map<String, Map<String, ColumnMetadata>> metaMap,
                                 Map<String, Map<String, Object>> storageMap,
                                 Map<String, List<String>> keyMap,
                                 String tableName,
                                 List<Tuple5> metaRecordList,
                                 List<Tuple2> storageList,
                                 List<String> keyList) {
        // column和column详细信息的映射
        Map<String, ColumnMetadata> tmpMetaMap = Maps.newHashMap();
        metaRecordList.stream().forEach(metaRecord -> {
            String columnName = (String) metaRecord.f0;
            DataType dataType = (DataType) metaRecord.f1;
            String description = (String) metaRecord.f2;
            boolean isKey = (boolean) metaRecord.f3;
            String alias = (String) metaRecord.f4;
            StaticColumnMetadata columnMeta = new StaticColumnMetadata(columnName,
                    dataType,
                    description,
                    isKey,
                    alias);
            tmpMetaMap.put(columnName, columnMeta);
        });
        metaMap.put(tableName, tmpMetaMap);

        //storage配置k->v映射
        Map<String, Object> tmpstorageMap = Maps.newHashMap();
        storageList.stream().forEach(storageConf -> {
            String confKey = (String) storageConf.f0;
            Object confValue = storageConf.f1;
            tmpstorageMap.put(confKey, confValue);
        });
        storageMap.put(tableName, tmpstorageMap);

        //key信息
        keyMap.put(tableName, keyList);
    }

    //初始化静态表591_test_ignite_write2
    private void initStaticTableIgniteWrite2(Map<String, Map<String, ColumnMetadata>> metaMap,
                                             Map<String, Map<String, Object>> storageMap,
                                             Map<String, List<String>> keyMap) {
        String tableName = "591_test_ignite_write2";
        //Tuple5: columnName, dataType, description, isKey, alias
        List<Tuple5> metaRecordList = Arrays.asList(Tuple5.of("path", DataType.STRING, "path", false, ""),
                Tuple5.of("gseindex", DataType.LONG, "gseindex", true, ""),
                Tuple5.of("_startTime_", DataType.STRING, "_startTime_", false, ""),
                Tuple5.of("report_time", DataType.STRING, "report_time", true, ""),
                Tuple5.of("log", DataType.STRING, "log", false, ""),
                Tuple5.of("ip", DataType.STRING, "ip", true, ""),
                Tuple5.of("_endTime_", DataType.STRING, "_endTime_", false, ""),
                Tuple5.of("timestamp", DataType.LONG, "timestamp", false, ""));
        //Tuple2 storageConf key value
        List<Tuple2> storageList = Arrays.asList(Tuple2.of("cache_name", "test_ignite_write2_591"),
                Tuple2.of("password", ""),
                Tuple2.of("kv_source_type", "ignite"),
                Tuple2.of("data_id", "591_test_ignite_write2"),
                Tuple2.of("port", 10800),
                Tuple2.of("ignite_cluster", "ignite-join-test"),
                Tuple2.of("redis_type", "private"),
                Tuple2.of("host", "0.0.0.0"),
                Tuple2.of("key_separator", ":"),
                Tuple2.of("ignite_user", "admin"),
                Tuple2.of("query_mode", "single"),
                Tuple2.of("key_order", new JSONArray("[\"gseindex\",\"ip\",\"report_time\"]")));
        // JOIN key
        List<String> keyList = Arrays.asList("ip", "report_time", "gseindex");
        initStaticTable(metaMap, storageMap, keyMap, tableName, metaRecordList, storageList, keyList);
    }

    //初始化静态表591_test_ignite_write_redis2_node
    private void initStaticIgniteWriteRedis2Node(Map<String, Map<String, ColumnMetadata>> metaMap,
                                                 Map<String, Map<String, Object>> storageMap,
                                                 Map<String, List<String>> keyMap) {
        String tableName = "591_test_ignite_write_redis2_node";
        //Tuple5: columnName, dataType, description, isKey, alias
        List<Tuple5> metaRecordList = Arrays.asList(Tuple5.of("path", DataType.STRING, "path", false, ""),
                Tuple5.of("gseindex", DataType.LONG, "gseindex", false, ""),
                Tuple5.of("_startTime_", DataType.STRING, "_startTime_", false, ""),
                Tuple5.of("report_time", DataType.STRING, "report_time", true, ""),
                Tuple5.of("log", DataType.STRING, "log", false, ""),
                Tuple5.of("ip", DataType.STRING, "ip", true, ""),
                Tuple5.of("_endTime_", DataType.STRING, "_endTime_", false, ""),
                Tuple5.of("timestamp", DataType.LONG, "timestamp", false, ""));
        //Tuple2 storageConf key value
        List<Tuple2> storageList = Arrays.asList(Tuple2.of("cache_name", "test_ignite_write_redis2_node_591"),
                Tuple2.of("password", ""),
                Tuple2.of("kv_source_type", "ignite"),
                Tuple2.of("data_id", "591_test_ignite_write_redis2_node"),
                Tuple2.of("port", 10800),
                Tuple2.of("ignite_cluster", "ignite-join-test"),
                Tuple2.of("redis_type", "private"),
                Tuple2.of("host", "0.0.0.0"),
                Tuple2.of("key_separator", ":"),
                Tuple2.of("ignite_user", "admin"),
                Tuple2.of("query_mode", "single"),
                Tuple2.of("key_order", new JSONArray("[\"ip\",\"report_time\"]")));
        // JOIN key
        List<String> keyList = Arrays.asList("ip", "report_time");
        initStaticTable(metaMap, storageMap, keyMap, tableName, metaRecordList, storageList, keyList);
    }

    //初始化静态表591_iplib
    private void initStaticTableIplib(Map<String, Map<String, ColumnMetadata>> metaMap,
                                      Map<String, Map<String, Object>> storageMap,
                                      Map<String, List<String>> keyMap) {
        String tableName = "591_iplib";
        //Tuple5: columnName, dataType, description, isKey, alias
        List<Tuple5> metaRecordList = Arrays.asList(Tuple5.of("path", DataType.STRING, "path", false, ""),
                Tuple5.of("gseindex", DataType.LONG, "gseindex", false, ""),
                Tuple5.of("report_time", DataType.STRING, "report_time", false, ""),
                Tuple5.of("log", DataType.STRING, "log", false, ""),
                Tuple5.of("ip", DataType.STRING, "ip", true, ""),
                Tuple5.of("timestamp", DataType.LONG, "timestamp", false, ""));
        //Tuple2 storageConf key value
        List<Tuple2> storageList = Arrays.asList(
                Tuple2.of("password", ""),
                Tuple2.of("kv_source_type", "ipv4"),
                Tuple2.of("data_id", "591_iplib"),
                Tuple2.of("port", 6380),
                Tuple2.of("redis_type", "private"),
                Tuple2.of("host", "0.0.0.0"),
                Tuple2.of("query_mode", "single"));
        // JOIN key
        List<String> keyList = Arrays.asList("ip");
        initStaticTable(metaMap, storageMap, keyMap, tableName, metaRecordList, storageList, keyList);
    }

    //初始化静态表591_f1630_s_02
    private void initStaticTablef1630(Map<String, Map<String, ColumnMetadata>> metaMap,
                                      Map<String, Map<String, Object>> storageMap,
                                      Map<String, List<String>> keyMap) {
        String tableName = "591_f1630_s_02";
        //Tuple5: columnName, dataType, description, isKey, alias
        List<Tuple5> metaRecordList = Arrays.asList(Tuple5.of("path", DataType.STRING, "path", true, ""),
                Tuple5.of("gseindex", DataType.LONG, "gseindex", true, ""),
                Tuple5.of("report_time", DataType.STRING, "report_time", false, ""),
                Tuple5.of("log", DataType.STRING, "log", true, ""),
                Tuple5.of("ip", DataType.STRING, "ip", false, ""),
                Tuple5.of("timestamp", DataType.LONG, "timestamp", false, ""));
        //Tuple2 storageConf key value
        List<Tuple2> storageList = Arrays.asList(
                Tuple2.of("password", ""),
                Tuple2.of("kv_source_type", ""),
                Tuple2.of("data_id", "591_f1630_s_02"),
                Tuple2.of("port", 30000),
                Tuple2.of("redis_type", "private"),
                Tuple2.of("host", "0.0.0.0"),
                Tuple2.of("query_mode", "single"));
        // JOIN key
        List<String> keyList = Arrays.asList("gseindex", "path", "log");
        initStaticTable(metaMap, storageMap, keyMap, tableName, metaRecordList, storageList, keyList);
    }

    private void mockCacheGenTable() throws Exception {
        Map<String, Map<String, ColumnMetadata>> metaMap = Maps.newHashMap();
        //填充general table cache测试数据
        initGenTablef1630(metaMap);
        initGenTableTestAppendx(metaMap);
        initGenTableEtlFlinkSql(metaMap);
        //mock 静态方法提前准备udf cache数据
        BlueKingTrtTableMetadataConnector mockGenTableConnector = PowerMockito
                .mock(BlueKingTrtTableMetadataConnector.class);
        // mock 获取meta方法
        PowerMockito.when(mockGenTableConnector.fetchTableMetadata(any()))
                .thenAnswer(new Answer<TableMetadata<ColumnMetadata>>() {
                    @Override
                    public TableMetadata<ColumnMetadata> answer(InvocationOnMock invocationOnMock)
                            throws Throwable {
                        String tableName = invocationOnMock.getArgument(0);
                        return MapBasedTableMetadata.wrap(metaMap.get(tableName));
                    }
                });
        //mock静态方法，任任意URL都返回提前准备好的Connector
        PowerMockito.mockStatic(BlueKingTrtTableMetadataConnector.class);
        PowerMockito.when(BlueKingTrtTableMetadataConnector.forUrl(anyString()))
                .thenReturn(mockGenTableConnector);
    }

    //初始化实时表591_f1630_s_02
    private void initGenTablef1630(Map<String, Map<String, ColumnMetadata>> metaMap) {
        String tableName = "591_f1630_s_02";
        //Tuple5: columnName, dataType, description, alias
        List<Tuple4> metaRecordList = Arrays.asList(Tuple4.of("path", DataType.STRING, "path", ""),
                Tuple4.of("gseindex", DataType.LONG, "gseindex", ""),
                Tuple4.of("report_time", DataType.STRING, "report_time", ""),
                Tuple4.of("log", DataType.STRING, "log", ""),
                Tuple4.of("ip", DataType.STRING, "ip", ""),
                Tuple4.of("timestamp", DataType.LONG, "timestamp", ""));
        initGenTable(metaMap, tableName, metaRecordList);
    }

    //初始化实时表591_test_append_x
    private void initGenTableTestAppendx(Map<String, Map<String, ColumnMetadata>> metaMap) {
        String tableName = "591_test_append_x";
        //Tuple5: columnName, dataType, description, alias
        List<Tuple4> metaRecordList = Arrays.asList(
                Tuple4.of("gseindey", DataType.LONG, "gseindey", ""),
                Tuple4.of("log", DataType.STRING, "log", ""),
                Tuple4.of("report_time", DataType.STRING, "report_time", ""),
                Tuple4.of("ip", DataType.STRING, "ip", ""),
                Tuple4.of("path_m", DataType.STRING, "path_m", ""),
                Tuple4.of("timestamp", DataType.LONG, "timestamp", ""));
        initGenTable(metaMap, tableName, metaRecordList);
    }

    //初始化实时表591_etl_flink_sql
    private void initGenTableEtlFlinkSql(Map<String, Map<String, ColumnMetadata>> metaMap) {
        String tableName = "591_etl_flink_sql";
        //Tuple5: columnName, dataType, description, alias
        List<Tuple4> metaRecordList = Arrays.asList(
                Tuple4.of("path", DataType.STRING, "path", "path"),
                Tuple4.of("gseindex", DataType.LONG, "gseindex", "gseindex"),
                Tuple4.of("log", DataType.STRING, "log", ""),
                Tuple4.of("report_time", DataType.STRING, "report_time", "report_time"),
                Tuple4.of("ip", DataType.STRING, "ip", "ip"),
                Tuple4.of("timestamp", DataType.LONG, "dtEventTime", "dtEventTime"));
        initGenTable(metaMap, tableName, metaRecordList);
    }

    private void initGenTable(Map<String, Map<String, ColumnMetadata>> metaMap,
                              String tableName,
                              List<Tuple4> metaRecordList) {
        // column和column详细信息的映射
        Map<String, ColumnMetadata> tmpMetaMap = Maps.newHashMap();
        metaRecordList.stream().forEach(metaRecord -> {
            String columnName = (String) metaRecord.f0;
            DataType dataType = (DataType) metaRecord.f1;
            String description = (String) metaRecord.f2;
            String alias = (String) metaRecord.f3;
            ColumnMetadataImpl columnMeta = new ColumnMetadataImpl(columnName,
                    dataType,
                    description,
                    alias);
            tmpMetaMap.put(columnName, columnMeta);
        });
        metaMap.put(tableName, tmpMetaMap);
    }
}
