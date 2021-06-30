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

import com.google.common.base.Preconditions;
import com.tencent.bk.base.datalab.bksql.exception.TokenMgrException;
import java.io.Reader;
import java.util.regex.Matcher;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.ParseException;
import org.apache.calcite.sql.parser.impl.Token;
import org.apache.calcite.sql.parser.impl.TokenMgrError;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.commons.lang.StringUtils;

public class ParserHelper {

    private static final java.util.regex.Pattern TOKEN_ERROR_PATTERN = java.util.regex.Pattern
            .compile(
                    "(?s)Lexical error at line ([0-9]+), column ([0-9]+).*");

    public static SqlNode parse(String sql) throws Exception {
        Preconditions.checkArgument(StringUtils.isNotBlank(sql), "sql can not be null or empty");
        SqlParser sqlParser = SqlParserFactory.getInstance().createParser(sql);
        return getSingleSqlNode(sqlParser);
    }

    public static SqlNode parse(Reader sql, FrameworkConfig config) throws Exception {
        Preconditions.checkArgument(sql != null, "sql can not be null or empty");
        SqlParser sqlParser = SqlParserFactory.getInstance().createParser(sql, config);
        return getSingleSqlNode(sqlParser);
    }

    public static SqlNode parse(String sql, FrameworkConfig config) throws Exception {
        Preconditions.checkArgument(StringUtils.isNotBlank(sql), "sql can not be null or empty");
        SqlParser sqlParser = SqlParserFactory.getInstance().createParser(sql, config);
        return getSingleSqlNode(sqlParser);
    }


    private static SqlNode getSingleSqlNode(SqlParser sqlParser) throws SqlParseException {
        SqlNode parsedNode = null;
        try {
            SqlNodeList nodeList = sqlParser.parseStmtList();
            Preconditions.checkArgument(nodeList != null);
            if (nodeList.size() == 1) {
                parsedNode = nodeList.get(0);
            } else {
                throw new RuntimeException("Multi-SQL parsing is not yet supported");
            }
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            Throwable cause = e.getCause();
            if (cause instanceof org.apache.calcite.sql.parser.impl.ParseException) {
                ParseException pe = (org.apache.calcite.sql.parser.impl.ParseException) cause;
                Token token = pe.currentToken.next;
                int beginLine = token.beginLine;
                int beginColumn = token.beginColumn;
                String image = token.image;
                throw new com.tencent.bk.base.datalab.bksql.exception.ParseException(image, beginLine,
                        beginColumn);
            } else if (cause instanceof org.apache.calcite.sql.parser.impl.TokenMgrError) {
                TokenMgrError te = (org.apache.calcite.sql.parser.impl.TokenMgrError) cause;
                Matcher matcher = TOKEN_ERROR_PATTERN.matcher(te.getMessage());
                if (matcher.matches()) {
                    int line = Integer.parseInt(matcher.group(1));
                    int column = Integer.parseInt(matcher.group(2));
                    throw new TokenMgrException(line, column);
                }
                throw new RuntimeException(te);
            } else {
                throw e;
            }
        } catch (RuntimeException e) {
            throw e;
        }
        return parsedNode;
    }
}
