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

package com.tencent.bk.base.dataflow.bksql.deparser;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParser.JoinType;
import com.tencent.blueking.bksql.exception.FailedOnDeParserException;
import com.tencent.bk.base.dataflow.bksql.util.GeneratedSchema;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class FlinkSqlDeParserJoin {

    /**
     * 查找实时JOIN中关联的key字段
     *
     * @param expression
     * @param array
     * @param leftAliasName
     * @param rightAliasName
     */
    public static void findJoinKeys(Expression expression,
                                    ArrayNode array,
                                    String leftAliasName,
                                    String rightAliasName) {
        Preconditions.checkNotNull(expression, "join keys");
        if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            Expression leftExpression = equalsTo.getLeftExpression();
            Expression rightExpression = equalsTo.getRightExpression();
            if (!(leftExpression instanceof Column && rightExpression instanceof Column)) {
                throw new IllegalArgumentException("please format join condition as: "
                        + "a.key_1 = b.key_1 AND a.key_2 = b.key_2");
            }
            Column leftColumn = (Column) leftExpression;
            Column rightColumn = (Column) rightExpression;
            String leftAliasNameOfAnd = FlinkSqlDeParser.getTableName(leftColumn.getTable());
            String rightAliasNameOfAnd = FlinkSqlDeParser.getTableName(rightColumn.getTable());
            ObjectNode obj = array.addObject();
            if (leftAliasNameOfAnd.equals(leftAliasName)) {
                obj.put("first", FlinkSqlDeParser.getColumnName(leftColumn));
                if (rightAliasNameOfAnd.equals(rightAliasName)) {
                    obj.put("second", FlinkSqlDeParser.getColumnName(rightColumn));
                } else {
                    throw new IllegalArgumentException("illegal join key reference");
                }
            } else if (leftAliasNameOfAnd.equals(rightAliasName)) {
                obj.put("first", FlinkSqlDeParser.getColumnName(rightColumn));
                if (rightAliasNameOfAnd.equals(leftAliasName)) {
                    obj.put("second", FlinkSqlDeParser.getColumnName(leftColumn));
                } else {
                    throw new IllegalArgumentException("illegal join key reference");
                }
            }
        } else if (expression instanceof AndExpression) {
            AndExpression and = (AndExpression) expression;
            findJoinKeys(and.getLeftExpression(), array, leftAliasName, rightAliasName);
            findJoinKeys(and.getRightExpression(), array, leftAliasName, rightAliasName);
        } else {
            throw new IllegalArgumentException("unrecognizable join condition: " + expression);
        }
    }

    /**
     * 查找静态JOIN中关联的key字段
     *
     * @param expression
     * @param sortedJoinKeys
     * @param staticAliasName
     * @param streamAliasName
     * @param staticTableKeys
     */
    public static void findStaticJoinKeys(Expression expression,
                                          List<Map<String, String>> sortedJoinKeys,
                                          String staticAliasName,
                                          String streamAliasName,
                                          List<String> staticTableKeys) {
        Preconditions.checkNotNull(expression, "join keys");
        if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            Expression leftExpression = equalsTo.getLeftExpression();
            Expression rightExpression = equalsTo.getRightExpression();
            if (!(leftExpression instanceof Column && rightExpression instanceof Column)) {
                throw new IllegalArgumentException("please format join condition as: "
                        + "a.key_1 = b.key_1 AND a.key_2 = b.key_2");
            }
            Column leftColumn = (Column) leftExpression;
            Column rightColumn = (Column) rightExpression;
            String leftAliasNameOfAnd = FlinkSqlDeParser.getTableName(leftColumn.getTable());
            String rightAliasNameOfAnd = FlinkSqlDeParser.getTableName(rightColumn.getTable());
            if (leftAliasNameOfAnd.equals(staticAliasName)) {
                int index = staticTableKeys.indexOf(FlinkSqlDeParser.getColumnName(leftColumn));
                if (index == -1) {
                    throw new FailedOnDeParserException("static.join.key.error",
                            new Object[]{FlinkSqlDeParser.getColumnName(leftColumn)}, FlinkSqlDeParser.class);
                }
                Map<String, String> obj = new HashMap<>();
                // 生成key的配置是带有顺序的
                sortedJoinKeys.set(index, obj);
                obj.put("static", FlinkSqlDeParser.getColumnName(leftColumn));
                if (rightAliasNameOfAnd.equals(streamAliasName)) {
                    obj.put("stream", FlinkSqlDeParser.getColumnName(rightColumn));
                } else {
                    throw new IllegalArgumentException("illegal join key reference");
                }
            } else if (leftAliasNameOfAnd.equals(streamAliasName)) {
                if (rightAliasNameOfAnd.equals(staticAliasName)) {
                    int index = staticTableKeys.indexOf(FlinkSqlDeParser.getColumnName(rightColumn));
                    if (index == -1) {
                        throw new FailedOnDeParserException("static.join.key.error",
                                new Object[]{FlinkSqlDeParser.getColumnName(rightColumn)}, FlinkSqlDeParser.class);
                    }
                    Map<String, String> obj = new HashMap<>();
                    sortedJoinKeys.set(index, obj);
                    obj.put("static", FlinkSqlDeParser.getColumnName(rightColumn));
                    obj.put("stream", FlinkSqlDeParser.getColumnName(leftColumn));
                } else {
                    throw new IllegalArgumentException("illegal join key reference");
                }
            }
        } else if (expression instanceof AndExpression) {
            AndExpression and = (AndExpression) expression;
            findStaticJoinKeys(and.getLeftExpression(),
                    sortedJoinKeys,
                    staticAliasName,
                    streamAliasName,
                    staticTableKeys);
            findStaticJoinKeys(and.getRightExpression(),
                    sortedJoinKeys,
                    staticAliasName,
                    streamAliasName,
                    staticTableKeys);
        } else {
            throw new IllegalArgumentException("unrecognizable join condition: " + expression);
        }
    }

    public static boolean isJoinQuery(PlainSelect select) {
        return CollectionUtils.isNotEmpty(select.getJoins())
                && select.getJoins().stream().filter(Join::isSimple)
                .collect(Collectors.toList()).isEmpty();
    }

    public static JoinType getJoinType(Join join) {
        if (join.isLeft()) {
            return JoinType.LEFT;
        }
        if (join.isRight()) {
            return JoinType.RIGHT;
        }
        if (join.isInner()) {
            return JoinType.INNER;
        }
        return JoinType.INNER;
    }

    /**
     * 判断equal表达式左右表别名是否包含事实表和静态表
     *
     * @param factAliasName
     * @param staticAliasName
     * @param equalsTo
     * @return true 表达式内的表别名包含事实表和静态表
     */
    public static boolean isJoinContainsAlias(String factAliasName, String staticAliasName, EqualsTo equalsTo) {
        Column leftExpression = (Column) equalsTo.getLeftExpression();
        Column rightExpression = (Column) equalsTo.getRightExpression();
        String leftAliasName = FlinkSqlDeParser.getAliasName(leftExpression.getTable());
        String rightAliasName = FlinkSqlDeParser.getAliasName(rightExpression.getTable());
        if ((StringUtils.equals(factAliasName, leftAliasName)
                || StringUtils.equals(factAliasName, rightAliasName))
                && (StringUtils.equals(staticAliasName, leftAliasName)
                || StringUtils.equals(staticAliasName, rightAliasName))) {
            return true;
        }
        return false;
    }

    /**
     * 根据 别名 查找所属表名称
     *
     * @param alias 显式指定的别名
     * @param staticTableAliaMap 静态表名和别名的映射关系
     * @return 别名对应的表名称
     */
    private static String queryTableNameByAlias(String alias, Map<String, String> staticTableAliaMap) {
        Map<String, String> finalStaticTableAliaMap = staticTableAliaMap
                .entrySet()
                .stream()
                .filter(entry -> alias.equalsIgnoreCase(entry.getValue()))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue(), (k, v) -> v));

        if (finalStaticTableAliaMap.size() == 1) {
            return (String) (finalStaticTableAliaMap.keySet().toArray()[0]);
        } else {
            throw new FailedOnDeParserException("alia.name.illegal.error",
                    new Object[]{alias}, FlinkSqlDeParser.class);
        }
    }

    /**
     * 从多个静态表中查找 列 所属表的名称
     *
     * @param factTableName 事实表
     * @param factAliasName 事实表别名
     * @param staticTableAliaMap 静态表与静态表别名的映射
     * @param column 列
     * @return tableName
     */
    public static String findFromMultiStaticTable(String factTableName,
                                                  String factAliasName,
                                                  Map<String, String> staticTableAliaMap,
                                                  Column column,
                                                  Map<String, GeneratedSchema> tableRegistry,
                                                  FlinkSqlDeParserValidate validate) {
        String fromTableName;
        String alias = FlinkSqlDeParser.getTableName(column.getTable(), true);
        String columnName = FlinkSqlDeParser.getColumnName(column);
        // 查询列未写别名
        if (alias == null) {
            fromTableName = queryTableNameByColumn(columnName,
                    factTableName,
                    staticTableAliaMap,
                    tableRegistry,
                    validate);
        } else if (alias.equalsIgnoreCase(factAliasName)) {
            fromTableName = factTableName;
        } else { // 查询列显式写别名
            fromTableName = queryTableNameByAlias(alias, staticTableAliaMap);
        }
        return fromTableName;
    }

    /**
     * 根据 列名 查找所属表名称
     *
     * @param columnName 指定列名
     * @param factTableName 事实表名
     * @param staticTableAliaMap 静态表名和别名的映射关系
     * @return table name
     */
    private static String queryTableNameByColumn(String columnName,
                                                 String factTableName,
                                                 Map<String, String> staticTableAliaMap,
                                                 Map<String, GeneratedSchema> tableRegistry,
                                                 FlinkSqlDeParserValidate validate) {
        GeneratedSchema factSchema = tableRegistry.get(factTableName);
        boolean isColumnInFactTable = factSchema.asMap().containsKey(columnName);
        // 查询出包含指定列名的静态表<tableName, schema>列表
        Map<String, GeneratedSchema> staticTableSchema = findColumnSchemaInStaticTables(columnName,
                staticTableAliaMap,
                tableRegistry);
        // 检查列名是否在所有表中不存在或者存在于多个静态表中
        validate.checkSelectColumnInTable(isColumnInFactTable,
                columnName,
                factTableName,
                factSchema,
                staticTableSchema);
        if (isColumnInFactTable) {
            return factTableName;
        } else {
            return (String) (staticTableSchema.keySet().toArray()[0]);
        }
    }


    /**
     * 根据 列名 查找所属静态表名称
     *
     * @param columnName 需要查找的列名称
     * @param staticTableAliaMap 静态表名和别名的映射关系
     * @return 包含指定列名的表及其表结构
     */
    private static Map<String, GeneratedSchema> findColumnSchemaInStaticTables(
            String columnName,
            Map<String, String> staticTableAliaMap,
            Map<String, GeneratedSchema> tableRegistry) {
        //全部静态表
        Map<String, GeneratedSchema> staticTableSchema = staticTableAliaMap
                .keySet().stream()
                .collect(Collectors.toMap(tableName -> tableName,
                        tableName -> tableRegistry.get(tableName),
                        (k, v) -> v));
        //包含指定列名的静态表
        Map<String, GeneratedSchema> finalStaticTableSchema = staticTableSchema
                .entrySet().stream()
                .filter(entry -> entry.getValue().asMap().containsKey(columnName))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue(), (k, v) -> v));
        return finalStaticTableSchema;
    }
}
