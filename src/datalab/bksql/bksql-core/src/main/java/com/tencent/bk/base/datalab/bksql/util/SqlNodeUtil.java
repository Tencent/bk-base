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

package com.tencent.bk.base.datalab.bksql.util;

import java.util.Stack;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class SqlNodeUtil {

    /**
     * 获取Sql语句中的常量
     *
     * @param value 常量节点
     * @return 常量值
     */
    public static String convertToString(SqlNode value) {
        if (value instanceof SqlCharStringLiteral) {
            return ((SqlCharStringLiteral) value).toValue();
        } else if (value instanceof SqlNumericLiteral) {
            return String.valueOf(((SqlNumericLiteral) value).toValue());
        } else {
            return "";
        }
    }

    /**
     * 生成and表达式
     *
     * @param expr1 表达式1
     * @param expr2 表达式2
     * @return
     */
    public static SqlBasicCall and(SqlNode expr1, SqlNode expr2) {
        SqlBasicCall andExpression = new SqlBasicCall(SqlStdOperatorTable.AND,
                new SqlNode[]{expr1, expr2}, expr1.getParserPosition());
        return andExpression;
    }

    /**
     * 生成or表达式
     *
     * @param expr1 表达式1
     * @param expr2 表达式2
     * @return
     */
    public static SqlBasicCall or(SqlNode expr1, SqlNode expr2) {
        SqlBasicCall orExpression = new SqlBasicCall(SqlStdOperatorTable.OR,
                new SqlNode[]{expr1, expr2}, expr1.getParserPosition());
        return orExpression;
    }

    /**
     * 生成not表达式
     *
     * @param expr 源表达式
     * @return
     */
    public static SqlBasicCall not(SqlNode expr) {
        SqlBasicCall notExpression = new SqlBasicCall(SqlStdOperatorTable.NOT, new SqlNode[]{expr},
                expr.getParserPosition());
        return notExpression;
    }

    /**
     * 还原Sql文本中的换行符
     *
     * @param sqlNode 当前节点
     * @param deparseStack 节点解析栈
     * @param buffer 当前已经还原的Sql字符串
     */
    public static void appendNewLine(SqlNode sqlNode, Stack<SqlNode> deparseStack,
            StringBuilder buffer) {
        if (sqlNode instanceof SqlOrderBy) {
            return;
        }
        if (!deparseStack.empty()) {
            SqlParserPos prePos = deparseStack.pop().getParserPosition();
            SqlParserPos curPos = sqlNode.getParserPosition();
            int preLineNum = prePos.getLineNum();
            int curLineNum = curPos.getLineNum();
            if (preLineNum * curLineNum == 0) {
                return;
            }
            if (prePos.getLineNum() != curPos.getLineNum()) {
                buffer.append("\r\n");
            }
        }
        deparseStack.push(sqlNode);
    }

    /**
     * 对字符串进行强制类型转换
     *
     * @param value 要转换的字符串
     * @param typeName 类型名称
     * @return cast 节点
     */
    public static SqlNode generateSqlCastNode(String value, SqlTypeName typeName) {
        SqlNode first = SqlLiteral.createCharString(value, SqlParserPos.ZERO);
        SqlBasicTypeNameSpec timeStampType = new SqlBasicTypeNameSpec(typeName, -1, 2,
                null, SqlParserPos.ZERO);
        SqlDataTypeSpec typeSpec = new SqlDataTypeSpec(timeStampType, SqlParserPos.ZERO);
        SqlNode[] newOperands = new SqlNode[2];
        newOperands[0] = first;
        newOperands[1] = typeSpec;
        SqlCastFunction castFunction = new SqlCastFunction();
        SqlBasicCall castCall = new SqlBasicCall(castFunction, newOperands, SqlParserPos.ZERO);
        return castCall;
    }
}
