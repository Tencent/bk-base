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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.blueking.bksql.validator.checker.AbsentAliasChecker;
import com.tencent.bk.base.dataflow.bksql.validator.checker.IcebergInvalidSQLChecker;
import com.tencent.bk.base.dataflow.bksql.validator.optimizer.SparkSqlIpLibOptimizer;
import com.tencent.bk.base.dataflow.bksql.validator.optimizer.SparkSqlLateralTableOptimizer;
import com.tencent.blueking.bksql.validator.optimizer.TableNameOptimizer;
import com.typesafe.config.ConfigFactory;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TaskTester {
    public static final String METADATA_URL = "http://127.0.0.1:8089/v3/meta/result_tables/{0}/?related=fields";
    private static final String DATAFLOW_UDF_URL = "http://127.0.0.1:8089/v3/dataflow/udf/functions/?env={0}&function_name={1}";

    private final String sql;
    private String expected = null;
    private String tableConf = null;

    public TaskTester(String sql) {
        this.sql = sql;
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
     * 触发测试单元测试SQL运行
     */
    public void run() throws Exception {
        List<String> udtfList = new ArrayList<>();
        udtfList.add("ipv4link_udtf(String)=[String, String, String, String, String, String, String, String, String]");
        udtfList.add("ipv6link_udtf(String)=[String, String, String, String, String, String, String, String, String]");
        udtfList.add("ipv4linkTgeoGenericBusiness_udtf(String)="
                + "[String, String, String, String, String, String, String, String, String]");
        udtfList.add("ipv6linkTgeoGenericBusiness_udtf(String)="
                + "[String, String, String, String, String, String, String, String, String]");
        udtfList.add("ipv4linkTgeoBaseNetwork_udtf(String)="
                + "[String, String, String, String, String, String, String, String, String]");
        udtfList.add("ipv6linkTgeoBaseNetwork_udtf(String)="
                + "[String, String, String, String, String, String, String, String, String]");

        Statement parsed = CCJSqlParserUtil.parse(sql);
        TableNameOptimizer tableNameOptimizer = new TableNameOptimizer(false, true);
        AbsentAliasChecker absentAliasChecker =
                new AbsentAliasChecker(Collections.emptyList(), Collections.emptyList());
        SparkSqlLateralTableOptimizer sparkSqlLateralTableOptimizer = new SparkSqlLateralTableOptimizer();

        absentAliasChecker.walk(parsed);
        tableNameOptimizer.walk(parsed);
        sparkSqlLateralTableOptimizer.walk(parsed);
        SparkSqlIpLibOptimizer sparkSqlIpLibOptimizer = new SparkSqlIpLibOptimizer();
        sparkSqlIpLibOptimizer.walk(parsed);
        IcebergInvalidSQLChecker icebergInvalidSQLChecker = new IcebergInvalidSQLChecker();
        icebergInvalidSQLChecker.walk(parsed);
        SparkSqlDeParser deParser = new SparkSqlDeParser(ConfigFactory.parseString(this.tableConf),
                METADATA_URL,
                DATAFLOW_UDF_URL,
                Collections.emptyList(),
                Collections.emptyList(),
                udtfList);
        Object deParsed = deParser.deParse(parsed);

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
}
