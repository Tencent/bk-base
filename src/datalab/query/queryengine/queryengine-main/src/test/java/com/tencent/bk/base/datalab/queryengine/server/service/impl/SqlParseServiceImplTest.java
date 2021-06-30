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

package com.tencent.bk.base.datalab.queryengine.server.service.impl;

import static org.hamcrest.core.Is.is;

import com.tencent.bk.base.datalab.queryengine.server.service.SqlParseService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@EnableAutoConfiguration
public class SqlParseServiceImplTest {

    @Autowired
    private SqlParseService sqlParseService;

    @Test
    public void syntaxCheck() throws Exception {
        String expect = "{\"keywordList\":{\"list\":[],\"vector\":false,\"pos\":"
                + "{\"lineNumber\":1,\"columnNumber\":1,\"endLineNumber\":1,"
                + "\"endColumnNumber\":6}},\"selectList\":{\"list\":[{\"names\":"
                + "[\"a\"],\"collation\":null,\"componentPositions\":[{\"lineNumber\":1,"
                + "\"columnNumber\":8,\"endLineNumber\":1,\"endColumnNumber\":8}],\"pos\":"
                + "{\"lineNumber\":1,\"columnNumber\":8,\"endLineNumber\":1,"
                + "\"endColumnNumber\":8}},{\"names\":[\"b\"],\"collation\":null,"
                + "\"componentPositions\":[{\"lineNumber\":1,\"columnNumber\":10,\"endLineNumber\""
                + ":1,\"endColumnNumber\":10}],\"pos\":{\"lineNumber\":1,\"columnNumber\":10,"
                + "\"endLineNumber\":1,\"endColumnNumber\":10}},{\"names\":[\"c\"],\"collation\":"
                + "null,\"componentPositions\":[{\"lineNumber\":1,\"columnNumber\":12,"
                + "\"endLineNumber\":1,\"endColumnNumber\":12}],\"pos\":{\"lineNumber\":1,"
                + "\"columnNumber\":12,\"endLineNumber\":1,\"endColumnNumber\":12}}],"
                + "\"vector\":false,\"pos\":{\"lineNumber\":1,\"columnNumber\":8,"
                + "\"endLineNumber\":1,\"endColumnNumber\":12}},\"from\":{\"names\":"
                + "[\"tab\"],\"collation\":null,\"componentPositions\":[{\"lineNumber\":"
                + "1,\"columnNumber\":19,\"endLineNumber\":1,\"endColumnNumber\":30}],\"pos\":"
                + "{\"lineNumber\":1,\"columnNumber\":19,\"endLineNumber\":1,\"endColumnNumber\":"
                + "30}},\"mySqlIndexHint\":null,\"where\":null,\"groupBy\":null,\"having\":null,"
                + "\"windowDecls\":{\"list\":[],\"vector\":false,\"pos\":{\"lineNumber\":1,"
                + "\"columnNumber\":1,\"endLineNumber\":1,\"endColumnNumber\":30}},\"orderBy\":"
                + "null,\"offset\":null,\"fetch\":null,\"pos\":{\"lineNumber\":1,\"columnNumber\":"
                + "1,\"endLineNumber\":1,\"endColumnNumber\":30}}";
        String sql = "select a,b,c from tab";
        String result = sqlParseService.checkSyntax(sql);
        Assert.assertThat(result, is(expect));
    }
}