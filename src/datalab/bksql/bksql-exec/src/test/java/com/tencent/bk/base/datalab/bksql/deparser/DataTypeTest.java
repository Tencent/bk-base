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

package com.tencent.bk.base.datalab.bksql.deparser;

import com.tencent.bk.base.datalab.bksql.protocol.BaseProtocolPlugin;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.typesafe.config.ConfigFactory;
import java.util.Locale;
import org.hamcrest.core.IsEqual;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataTypeTest extends DeParserTestSupport {

    @BeforeClass
    public static void beforeClass() {
        LocaleHolder.instance()
                .set(Locale.US);
    }

    @Test
    public void testBoolean() throws Exception {
        String sql = "SELECT cast(id as boolean) from tab";
        String expected = "SELECT CAST(id AS BOOLEAN) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testInteger() throws Exception {
        String sql = "SELECT cast(id as integer) from tab";
        String expected = "SELECT CAST(id AS INTEGER) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testInt() throws Exception {
        String sql = "SELECT cast(id as int) from tab";
        String expected = "SELECT CAST(id AS INTEGER) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testTinyInt() throws Exception {
        String sql = "SELECT cast(id as tinyint) from tab";
        String expected = "SELECT CAST(id AS TINYINT) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testBigInt() throws Exception {
        String sql = "SELECT cast(id as bigint) from tab";
        String expected = "SELECT CAST(id AS BIGINT) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testLong() throws Exception {
        String sql = "SELECT cast(id as long) from tab";
        String expected = "SELECT CAST(id AS long) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testFloat() throws Exception {
        String sql = "SELECT cast(id as float) from tab";
        String expected = "SELECT CAST(id AS FLOAT) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDouble() throws Exception {
        String sql = "SELECT cast(id as double) from tab";
        String expected = "SELECT CAST(id AS DOUBLE) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testREAL() throws Exception {
        String sql = "SELECT cast(id as real) from tab";
        String expected = "SELECT CAST(id AS REAL) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDecimal() throws Exception {
        String sql = "SELECT cast(id as decimal(1,1)) from tab";
        String expected = "SELECT CAST(id AS DECIMAL(1, 1)) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testVarchar1() throws Exception {
        String sql = "SELECT cast(id as varchar) from tab";
        String expected = "SELECT CAST(id AS VARCHAR) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testVarchar2() throws Exception {
        String sql = "SELECT cast(id as varchar(11)) from tab";
        String expected = "SELECT CAST(id AS VARCHAR(11)) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testChar() throws Exception {
        String sql = "SELECT cast(id as char) from tab";
        String expected = "SELECT CAST(id AS CHAR) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testString() throws Exception {
        String sql = "SELECT cast(id as string) from tab";
        String expected = "SELECT CAST(id AS string) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testBinary() throws Exception {
        String sql = "SELECT cast(id as binary) from tab";
        String expected = "SELECT CAST(id AS BINARY) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testVarBinary() throws Exception {
        String sql = "SELECT cast(id as varbinary) from tab";
        String expected = "SELECT CAST(id AS VARBINARY) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testJson() throws Exception {
        String sql = "SELECT cast(id as json) from tab";
        String expected = "SELECT CAST(id AS json) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDate() throws Exception {
        String sql = "SELECT cast(id as date) from tab";
        String expected = "SELECT CAST(id AS DATE) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testTime() throws Exception {
        String sql = "SELECT cast(id as time) from tab";
        String expected = "SELECT CAST(id AS TIME) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testTimeStamp() throws Exception {
        String sql = "SELECT cast(id as timestamp) from tab";
        String expected = "SELECT CAST(id AS TIMESTAMP) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testDateTime() throws Exception {
        String sql = "SELECT cast(id as datetime) from tab";
        String expected = "SELECT CAST(id AS datetime) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testArray1() throws Exception {
        String sql = "SELECT cast(id as array(int)) from tab";
        String expected = "SELECT CAST(id AS ARRAY(INTEGER)) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testArray2() throws Exception {
        String sql = "SELECT cast(id as int array) from tab";
        String expected = "SELECT CAST(id AS ARRAY(INTEGER)) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testArray3() throws Exception {
        String sql = "SELECT cast(id as array(real)) from tab";
        String expected = "SELECT CAST(id AS ARRAY(REAL)) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }
}
