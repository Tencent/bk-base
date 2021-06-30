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

package com.tencent.bk.base.datalab.bksql.validator.optimizer;

import com.tencent.bk.base.datalab.bksql.validator.ExpressionReplacementOptimizer;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class DivisionOptimizer extends ExpressionReplacementOptimizer {

    /**
     * 整数相除结果为整数，会导致用户使用问题，此处将被除数转成double类型
     *
     * @param divide 除法表达式
     * @return Expression
     */
    @Override
    protected SqlBasicCall enterDivideExpression(SqlBasicCall divide) {
        SqlNode[] initOperands = new SqlNode[2];
        initOperands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        initOperands[1] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlBasicCall copyExp = new SqlBasicCall(SqlStdOperatorTable.DIVIDE, initOperands,
                SqlParserPos.ZERO);
        SqlNode[] operands = copyExp.getOperands();
        if (operands != null) {
            SqlNode first = divide.operands[0];
            SqlBasicTypeNameSpec doubleType = new SqlBasicTypeNameSpec(SqlTypeName.DOUBLE, -1, 2,
                    null, SqlParserPos.ZERO);
            SqlDataTypeSpec typeSpec = new SqlDataTypeSpec(doubleType, SqlParserPos.ZERO);
            SqlNode[] newOperands = new SqlNode[2];
            newOperands[0] = first;
            newOperands[1] = typeSpec;
            SqlCastFunction castFunction = new SqlCastFunction();
            SqlBasicCall castCall = new SqlBasicCall(castFunction, newOperands, SqlParserPos.ZERO);
            copyExp.setOperand(0, castCall);
            copyExp.setOperand(1, divide.operands[1]);
        }
        return copyExp;
    }
}