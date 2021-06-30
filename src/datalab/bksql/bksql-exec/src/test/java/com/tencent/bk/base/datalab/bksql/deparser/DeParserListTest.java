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
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.Map;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Ignore;
import org.junit.Test;

public class DeParserListTest extends DeParserTestSupport {

    @Test
    @Ignore
    public void testSql() throws Exception {
        String sql = "SELECT col FROM tab";
        assertThat(new DeParserList(Arrays.asList(new Ast(),
                new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG))), sql,
                new BaseMatcher<Object>() {
                    @Override
                    public void describeTo(Description description) {

                    }

                    @Override
                    public boolean matches(Object item) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> m = (Map<String, Object>) item;
                        return m.size() == 2;
                    }
                });
    }
}