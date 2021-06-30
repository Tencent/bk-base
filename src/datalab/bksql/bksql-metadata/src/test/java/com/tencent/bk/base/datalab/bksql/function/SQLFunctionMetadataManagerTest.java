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

package com.tencent.bk.base.datalab.bksql.function;

import com.tencent.bk.base.datalab.bksql.util.DataType;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SQLFunctionMetadataManagerTest {

    private SQLFunctionMetadataManager manager;
    private Connection conn;

    @Before
    public void setUp() throws Exception {
        String url = "jdbc:h2:mem:test_db";
        conn = DriverManager.getConnection(url);
        manager = new SQLFunctionMetadataManager(url, "test_table");
    }

    @Test
    public void addFunctionMetadata() throws Exception {
        Assert.assertEquals(new Integer(1),
                manager.writer().addFunctionMetadata(new FunctionMetadata(
                        "func",
                        "test func",
                        Collections.singletonList(
                                new ParameterMetadata("param_0", DataType.STRING, false)),
                        DataType.STRING,
                        "namespace"
                )));
    }

    @Test
    public void fetchFunctionMetadata() throws Exception {
        manager.writer().addFunctionMetadata(new FunctionMetadata(
                "func",
                "test func",
                Collections.singletonList(new ParameterMetadata("param_0", DataType.STRING, false)),
                DataType.STRING,
                "namespace"
        ));
        List<FunctionMetadata> functionMetadata = manager
                .reader(Collections.singletonList("namespace")).fetchFunctionMetadata("func");
        Assert.assertEquals(functionMetadata, Collections.singletonList(new FunctionMetadata(
                "func",
                "test func",
                Collections.singletonList(new ParameterMetadata("param_0", DataType.STRING, false)),
                DataType.STRING,
                "namespace"
        )));
    }

    @Test
    public void fetchAllFunctionMetadata() throws Exception {
        manager.writer().addFunctionMetadata(new FunctionMetadata(
                "func",
                "test func",
                Collections.singletonList(new ParameterMetadata("param_0", DataType.STRING, false)),
                DataType.STRING,
                "namespace"
        ));
        List<FunctionMetadata> functionMetadata = manager
                .reader(Collections.singletonList("namespace")).fetchAllFunctionMetadata();
        Assert.assertEquals(functionMetadata, Collections.singletonList(new FunctionMetadata(
                "func",
                "test func",
                Collections.singletonList(new ParameterMetadata("param_0", DataType.STRING, false)),
                DataType.STRING,
                "namespace"
        )));
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }
}