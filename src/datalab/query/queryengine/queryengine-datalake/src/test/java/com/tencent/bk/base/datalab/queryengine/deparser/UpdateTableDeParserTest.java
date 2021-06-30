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

package com.tencent.bk.base.datalab.queryengine.deparser;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tencent.bk.base.datahub.iceberg.functions.ValFunction;
import com.tencent.bk.base.datalab.bksql.deparser.DeParserTestSupport;
import com.tencent.bk.base.datalab.bksql.parser.calcite.ParserHelper;
import com.tencent.bk.base.datalab.meta.Field;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.expressions.Expression;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpdateTableDeParserTest extends DeParserTestSupport {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static Config config = null;

    static {
        JSON_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JSON_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    /**
     * 测试开始前，准备样本数据
     */
    @BeforeClass
    public static void beforeClass() throws JsonProcessingException {
        Field id = new Field();
        id.setFieldName("id");
        id.setFieldAlias("id");
        id.setFieldIndex(0);
        id.setFieldType("int");

        Field name = new Field();
        name.setFieldName("name");
        name.setFieldAlias("name");
        name.setFieldIndex(1);
        name.setFieldType("string");

        List<Field> fields = new ArrayList<>();
        fields.add(id);
        fields.add(name);

        Properties properties = new Properties();
        properties.putIfAbsent("fields", JSON_MAPPER.writeValueAsString(fields));
        config = ConfigFactory.parseProperties(properties);
    }

    @Test
    public void testSingle() throws Exception {
        String sql = "update tab set name='a',id=123 where id = 1";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new UpdateTableDeParser(config).deParse(parsed);
        Map<String, Object> deParsedMap = (Map<String, Object>) deParsed;
        Map<String, ValFunction> assign = (Map<String, ValFunction>) deParsedMap.get("assign");
        Expression condition = (Expression) deParsedMap.get("condition");
        Assert.assertEquals("ref(name=\"id\") == 1", condition.toString());
        Assert.assertNotNull(assign.get("name"));
    }

    @Test
    public void testWhereAnd() throws Exception {
        String sql = "update tab set name='a',id=123 where id = 1 and name='test'";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new UpdateTableDeParser(config).deParse(parsed);
        Map<String, Object> deParsedMap = (Map<String, Object>) deParsed;
        Map<String, ValFunction> assign = (Map<String, ValFunction>) deParsedMap.get("assign");
        Expression condition = (Expression) deParsedMap.get("condition");
        Assert.assertEquals("(ref(name=\"id\") == 1 and ref(name=\"name\") == \"test\")",
                condition.toString());
        Assert.assertNotNull(assign.get("name"));
    }

    @Test
    public void testWhereOr() throws Exception {
        String sql = "update tab set name='a',id=123 where id = 1 or name='test'";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new UpdateTableDeParser(config).deParse(parsed);
        Map<String, Object> deParsedMap = (Map<String, Object>) deParsed;
        Map<String, ValFunction> assign = (Map<String, ValFunction>) deParsedMap.get("assign");
        Expression condition = (Expression) deParsedMap.get("condition");
        Assert.assertEquals("(ref(name=\"id\") == 1 or ref(name=\"name\") == \"test\")",
                condition.toString());
        Assert.assertNotNull(assign.get("name"));
        Assert.assertNotNull(assign.get("id"));
    }

    @Test(expected = RuntimeException.class)
    public void testException() throws Exception {
        String sql = "update tab set age=123 where id = 1 or name='test'";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new UpdateTableDeParser(config).deParse(parsed);
        Map<String, Object> deParsedMap = (Map<String, Object>) deParsed;
        Assert.assertNotNull(deParsedMap);
    }
}
