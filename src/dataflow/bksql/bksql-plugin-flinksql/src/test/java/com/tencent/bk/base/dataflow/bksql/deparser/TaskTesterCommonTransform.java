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
import static org.mockito.ArgumentMatchers.anyString;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.dataflow.bksql.util.GeneratedSchema;
import com.tencent.blueking.bksql.function.udf.BkdataUdfMetadataConnector;
import com.tencent.blueking.bksql.function.udf.UdfArgs;
import com.tencent.blueking.bksql.function.udf.UdfMetadata;
import com.tencent.blueking.bksql.function.udf.UdfParameterMetadata;
import com.tencent.blueking.bksql.table.BlueKingTrtTableMetadataConnector;
import com.tencent.blueking.bksql.table.ColumnMetadata;
import com.tencent.blueking.bksql.table.ColumnMetadataImpl;
import com.tencent.blueking.bksql.table.MapBasedTableMetadata;
import com.tencent.blueking.bksql.table.TableMetadata;
import com.tencent.blueking.bksql.util.DataType;
import com.tencent.blueking.bksql.util.UdfType;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;

public class TaskTesterCommonTransform {

    private final String sql;
    private String expected = null;
    private String tableConf = null;

    public TaskTesterCommonTransform(String sql) {
        this.sql = sql;
    }

    public TaskTesterCommonTransform setTableConf(String tableConf) {
        this.tableConf = tableConf;
        return this;
    }

    public TaskTesterCommonTransform taskContains(String expected) throws IOException {
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
        //mock测试genTable cache数据
        mockCacheGenTableInfo();
        FlinkSqlDeParserAPIV3 deParserAPIV3 = new FlinkSqlDeParserAPIV3(
                ConfigFactory.parseString(this.tableConf),
                METADATA_URL,
                STATIC_METADATA_URL,
                UDF_METADATA_URL,
                Arrays.asList("COUNT", "AVG", "SUM", "MAX", "MIN", "BKDATA_LAST", "BKDATA_SUM", "GROUP_CONCAT"));
        // mock测试表数据
        mockTableDataInfo(deParserAPIV3);
        TaskTester.runTestCase(sql, deParserAPIV3, expected);
    }

    private void mockCacheUdfInfo() throws Exception {
        //填充udf测试数据
        List<UdfParameterMetadata> parameterMetadataList = Lists.newArrayList();
        parameterMetadataList
                .add(new UdfParameterMetadata(Arrays.asList("string", "string"), Arrays.asList("string", "int")));
        UdfMetadata udfMetadata = new UdfMetadata("udf_java_udtf", UdfType.UDTF, parameterMetadataList);
        //key
        UdfArgs udfArgs = new UdfArgs("dev", "udf_java_udtf");
        //value
        List<UdfMetadata> udfMetaList = Lists.newArrayList();
        udfMetaList.add(udfMetadata);

        //mock 静态方法提前准备udf cache数据
        BkdataUdfMetadataConnector mockUdfConnector = PowerMockito.mock(BkdataUdfMetadataConnector.class);
        PowerMockito.when(mockUdfConnector.fetchUdfMetaData(Matchers.argThat(new UdfArgsArgumentMatcher(udfArgs))))
                .thenReturn(udfMetaList);
        //mock静态方法，任任意URL都返回提前准备好的UdfConnector
        PowerMockito.mockStatic(BkdataUdfMetadataConnector.class);
        PowerMockito.when(BkdataUdfMetadataConnector.forUrl(anyString())).thenReturn(mockUdfConnector);
    }

    private void mockCacheGenTableInfo() throws Exception {
        //填充general table测试数据
        //value
        Map<String, ColumnMetadata> generalDataMap = Maps.newHashMap();
        generalDataMap.put("path",
                new ColumnMetadataImpl("path", DataType.STRING, "path", "path"));
        generalDataMap.put("gseindex",
                new ColumnMetadataImpl("gseindex", DataType.LONG, "gseindex", "gseindex"));
        generalDataMap.put("log",
                new ColumnMetadataImpl("log", DataType.STRING, "log", "log"));
        generalDataMap.put("report_time",
                new ColumnMetadataImpl("report_time", DataType.STRING, "report_time", "report_time"));
        generalDataMap.put("ip",
                new ColumnMetadataImpl("ip", DataType.STRING, "ip", "ip"));
        generalDataMap.put("timestamp",
                new ColumnMetadataImpl("timestamp", DataType.LONG, "dtEventTime", "dtEventTime"));
        TableMetadata tableMetadata = MapBasedTableMetadata.wrap(generalDataMap);
        //key
        String tableName = "591_etl_flink_sql";
        //mock 静态方法提前准备cache数据
        BlueKingTrtTableMetadataConnector mockGenTableConnector = PowerMockito
                .mock(BlueKingTrtTableMetadataConnector.class);
        PowerMockito.when(mockGenTableConnector.fetchTableMetadata(tableName)).thenReturn(tableMetadata);
        //mock静态方法，任任意URL都返回提前准备好的UdfConnector
        PowerMockito.mockStatic(BlueKingTrtTableMetadataConnector.class);
        PowerMockito.when(BlueKingTrtTableMetadataConnector.forUrl(anyString())).thenReturn(mockGenTableConnector);
    }


    private void mockTableDataInfo(FlinkSqlDeParserAPIV3 deParserAPIV3)
            throws NoSuchFieldException, IllegalAccessException {
        //mock 注册后带有开始结束时间的表
        Field field = deParserAPIV3.getClass().getSuperclass().getDeclaredField("tableRegistry");
        field.setAccessible(true);
        GeneratedSchema schemaMock = new GeneratedSchema(Lists.newArrayList());
        schemaMock.getColumnMetadata().add(new ColumnMetadataImpl("timestamp", DataType.LONG));
        schemaMock.getColumnMetadata().add(new ColumnMetadataImpl("dtEventTime", DataType.STRING));
        schemaMock.getColumnMetadata().add(new ColumnMetadataImpl("_startTime_", DataType.STRING));
        schemaMock.getColumnMetadata().add(new ColumnMetadataImpl("_endTime_", DataType.STRING));
        schemaMock.getColumnMetadata().add(new ColumnMetadataImpl("path", DataType.STRING));
        schemaMock.getColumnMetadata().add(new ColumnMetadataImpl("log", DataType.STRING));
        Map<String, GeneratedSchema> schemas = (Map<String, GeneratedSchema>) field.get(deParserAPIV3);
        schemas.put("591_etl_flink_sql", schemaMock);
    }
}
