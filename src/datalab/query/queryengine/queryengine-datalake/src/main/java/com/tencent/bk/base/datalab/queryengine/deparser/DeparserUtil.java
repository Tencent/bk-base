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

import com.tencent.bk.base.datalab.meta.Field;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

public class DeparserUtil {

    /**
     * 获取二元表达式的字段名和值
     *
     * @param call 二元表达式
     * @param fieldList 结果表字段列表
     * @return 包含字段和值的数组
     */
    public static Object[] getSimpleBinaryNameAndValue(SqlBasicCall call, List<Field> fieldList) {
        SqlNode firstNode = call.operands[0];
        SqlNode secondNode = call.operands[1];
        String columnName;
        Object columnValue;
        boolean isSimpleExp =
                firstNode instanceof SqlIdentifier && secondNode instanceof SqlLiteral;
        if (!isSimpleExp) {
            throw new RuntimeException("Not support expression");
        }
        columnName = ((SqlIdentifier) firstNode).getSimple();
        columnValue = getColumnValue(fieldList, secondNode, columnName);
        return new Object[]{columnName, columnValue};
    }

    /**
     * 解析 where 条件表达式，返回iceberg Expression实例
     *
     * @param condition where 条件表达式
     * @param fieldList 结果表字段列表
     * @return iceberg Expression实例
     */
    public static Expression resolveWhereCondition(SqlBasicCall condition, List<Field> fieldList) {
        Object[] nameValueArray;
        Expression finalExpression;
        switch (condition.getKind()) {
            case AND:
                finalExpression = Expressions
                        .and(resolveWhereCondition((SqlBasicCall) condition.operands[0], fieldList),
                                resolveWhereCondition((SqlBasicCall) condition.operands[1],
                                        fieldList));
                break;
            case OR:
                finalExpression = Expressions
                        .or(resolveWhereCondition((SqlBasicCall) condition.operands[0], fieldList),
                                resolveWhereCondition((SqlBasicCall) condition.operands[1],
                                        fieldList));
                break;
            case EQUALS:
                nameValueArray = getSimpleBinaryNameAndValue(condition, fieldList);
                finalExpression = Expressions
                        .equal(nameValueArray[0].toString(), nameValueArray[1]);
                break;
            case NOT_EQUALS:
                nameValueArray = getSimpleBinaryNameAndValue(condition, fieldList);
                finalExpression = Expressions
                        .notEqual(nameValueArray[0].toString(), nameValueArray[1]);
                break;
            case GREATER_THAN:
                nameValueArray = getSimpleBinaryNameAndValue(condition, fieldList);
                finalExpression = Expressions
                        .greaterThan(nameValueArray[0].toString(), nameValueArray[1]);
                break;
            case GREATER_THAN_OR_EQUAL:
                nameValueArray = getSimpleBinaryNameAndValue(condition, fieldList);
                finalExpression = Expressions
                        .greaterThanOrEqual(nameValueArray[0].toString(), nameValueArray[1]);
                break;
            case LESS_THAN:
                nameValueArray = getSimpleBinaryNameAndValue(condition, fieldList);
                finalExpression = Expressions
                        .lessThan(nameValueArray[0].toString(), nameValueArray[1]);
                break;
            case LESS_THAN_OR_EQUAL:
                nameValueArray = getSimpleBinaryNameAndValue(condition, fieldList);
                finalExpression = Expressions
                        .lessThanOrEqual(nameValueArray[0].toString(), nameValueArray[1]);
                break;
            default:
                throw new RuntimeException(
                        String.format("Not support sqlKind:%s", condition.getKind()));
        }
        return finalExpression;
    }

    /**
     * 获取字段值
     *
     * @param fieldList 字段列表
     * @param valueNode 字段值节点
     * @param columnName 字段名
     * @return 字段值
     */
    public static Object getColumnValue(List<Field> fieldList, SqlNode valueNode,
            String columnName) {
        Object columnValue;
        if (!(valueNode instanceof SqlCharStringLiteral)
                && !(valueNode instanceof SqlNumericLiteral)) {
            throw new RuntimeException(
                    String.format("Not support dataType for column:%s", columnName));
        }
        if (valueNode instanceof SqlCharStringLiteral) {
            return ((SqlCharStringLiteral) valueNode).getValue().toString()
                    .replace("'", "");
        }
        if (valueNode instanceof SqlNumericLiteral) {
            String dataType = "";
            for (Field f : fieldList) {
                if (f.getFieldName().equalsIgnoreCase(columnName)) {
                    dataType = f.getFieldType();
                    break;
                }
            }
            switch (dataType) {
                case "long":
                    columnValue = ((SqlNumericLiteral) valueNode).longValue(false);
                    break;
                case "int":
                    columnValue = ((SqlNumericLiteral) valueNode).intValue(false);
                    break;
                case "double":
                    columnValue = ((SqlNumericLiteral) valueNode).bigDecimalValue().doubleValue();
                    break;
                default:
                    throw new RuntimeException(
                            String.format("Bad column type: %s for column: %s", dataType,
                                    columnName));
            }
            return columnValue;
        }
        return null;
    }
}
