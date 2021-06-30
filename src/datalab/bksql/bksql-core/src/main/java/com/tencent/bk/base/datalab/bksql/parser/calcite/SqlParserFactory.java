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

package com.tencent.bk.base.datalab.bksql.parser.calcite;


import java.io.Reader;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.SourceStringReader;

public class SqlParserFactory {

    private static FrameworkConfig DEFAULT_CONFIG = Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true))
            .parserConfig(SqlParser.configBuilder()
                    .setParserFactory(SqlParserImpl.FACTORY)
                    .setCaseSensitive(false)
                    .setQuoting(Quoting.BACK_TICK)
                    .setQuotedCasing(Casing.UNCHANGED)
                    .setUnquotedCasing(Casing.UNCHANGED)
                    .setConformance(SqlConformanceEnum.LENIENT)
                    .build()).build();

    private SqlParserFactory() {
    }

    public static final SqlParserFactory getInstance() {
        return SqlParserFactoryHolder.INSTANCE;
    }

    public static FrameworkConfig getDefaultConfig() {
        return DEFAULT_CONFIG;
    }

    public SqlParser createParser(String sql, FrameworkConfig config) {
        return createParser((new SourceStringReader(sql)), config);
    }

    public SqlParser createParser(String sql) {
        return SqlParser.create(sql, DEFAULT_CONFIG.getParserConfig());
    }

    public SqlParser createParser(Reader reader, FrameworkConfig config) {
        if (config == null) {
            config = DEFAULT_CONFIG;
        }
        return SqlParser.create(reader, config.getParserConfig());
    }

    private static class SqlParserFactoryHolder {

        private static final SqlParserFactory INSTANCE = new SqlParserFactory();
    }
}
