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
import com.tencent.bk.base.datalab.bksql.deparser.DeParserTestSupport;
import com.tencent.bk.base.datalab.bksql.parser.calcite.ParserHelper;
import com.tencent.bk.base.datalab.meta.Field;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.expressions.Expression;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteTableDeparserTest extends DeParserTestSupport {

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

        Field salary = new Field();
        salary.setFieldName("salary");
        salary.setFieldAlias("salary");
        salary.setFieldIndex(1);
        salary.setFieldType("double");

        Field point = new Field();
        point.setFieldName("point");
        point.setFieldAlias("point");
        point.setFieldIndex(2);
        point.setFieldType("long");

        List<Field> fields = new ArrayList<>();

        fields.add(id);
        fields.add(salary);
        fields.add(point);
        Properties properties = new Properties();
        properties.putIfAbsent("fields", JSON_MAPPER.writeValueAsString(fields));
        config = ConfigFactory.parseProperties(properties);
    }

    @Test
    public void testEq() throws Exception {
        String sql = "delete from tab where id = 1";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("ref(name=\"id\") == 1", exp.toString());
    }

    @Test
    public void testNotEq() throws Exception {
        String sql = "delete from tab where id != 1";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("ref(name=\"id\") != 1", exp.toString());
    }

    @Test
    public void testGt() throws Exception {
        String sql = "delete from tab where id > 1";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("ref(name=\"id\") > 1", exp.toString());
    }

    @Test
    public void testGte() throws Exception {
        String sql = "delete from tab where id >= 1";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("ref(name=\"id\") >= 1", exp.toString());
    }

    @Test
    public void testLt() throws Exception {
        String sql = "delete from tab where id < 1";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("ref(name=\"id\") < 1", exp.toString());
    }

    @Test
    public void testLte() throws Exception {

        String sql = "delete from tab where id <= 1";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("ref(name=\"id\") <= 1", exp.toString());
    }

    @Test(expected = RuntimeException.class)
    public void testSubQuery() throws Exception {
        String sql = "delete from tab where id = (select * from tb_ids)";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Assert.fail("Not support complex expression");
    }

    @Test
    public void testAnd() throws Exception {
        String sql = "delete from tab where id = '1' and name='test'";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("(ref(name=\"id\") == \"1\" and ref(name=\"name\") == \"test\")",
                exp.toString());
    }

    @Test
    public void testOr() throws Exception {
        String sql = "delete from tab where id = 1 or name='test'";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("(ref(name=\"id\") == 1 or ref(name=\"name\") == \"test\")",
                exp.toString());
    }

    @Test(expected = RuntimeException.class)
    public void testAndOr() throws Exception {
        String sql = "delete from tab where id = 1 and (name='test' or age>18)";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals(
                "(ref(name=\"id\") == 1 and (ref(name=\"name\") == "
                        + "\"test\" or ref(name=\"age\") > 18))",
                exp.toString());
    }

    @Test(expected = RuntimeException.class)
    public void testBoolean() throws Exception {
        String sql = "delete from tab where id = true";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("ref(name=\"id\") == 1", exp.toString());
    }

    @Test
    public void testDouble() throws Exception {
        String sql = "delete from tab where salary = 10001.11";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("ref(name=\"salary\") == 10001.11", exp.toString());
    }

    @Test
    public void testLong() throws Exception {
        String sql = "delete from tab where point = 1111";
        SqlNode parsed = ParserHelper.parse(sql);
        Object deParsed = new DeleteTableDeparser(config).deParse(parsed);
        Expression exp = (Expression) deParsed;
        Assert.assertEquals("ref(name=\"point\") == 1111", exp.toString());
    }
}
