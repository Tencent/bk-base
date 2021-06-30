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

import static com.tencent.bk.base.dataflow.bksql.util.Constants.ACCUMULATE_WINDOW;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.GENERATE_FLINKSQL_JOIN_TABLE_SUFFIX;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.GENERATE_FLINKSQL_STATIC_JOIN_TABLE_SUFFIX;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.GENERATE_FLINKSQL_SUBQUERY_TABLE_SUFFIX;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_IS_STATIC_DATA;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_WINDOW_TYPE;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.SESSION_WINDOW;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.SLIDING_WINDOW;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.TUMBLING_WINDOW;

import com.google.common.collect.ImmutableSet;
import com.tencent.blueking.bksql.exception.FailedOnDeParserException;
import com.tencent.blueking.bksql.util.AggregationExpressionDetector;
import com.tencent.bk.base.dataflow.bksql.util.GeneratedSchema;
import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.collections.CollectionUtils;

public class FlinkSqlDeParserValidate {

    /**
     * 校验窗口类型与SQL是否匹配
     *
     * @param select
     */
    public void checkIsLegalWindowType(PlainSelect select,
                                       List<String> sourceData,
                                       Config config,
                                       AggregationExpressionDetector aggrExpDetector) {
        String windowType = config.hasPath(PROPERTY_KEY_WINDOW_TYPE)
                ? config.getString(PROPERTY_KEY_WINDOW_TYPE) : null;
        if (TUMBLING_WINDOW.equalsIgnoreCase(windowType)
                || SLIDING_WINDOW.equalsIgnoreCase(windowType)
                || SESSION_WINDOW.equalsIgnoreCase(windowType)
                || ACCUMULATE_WINDOW.equalsIgnoreCase(windowType)) {
            /*
             *  后面makeEntity会单独检查配置两个实时数据源时但SQL只使用一个表的情况
             *  此处忽略子查询验证
             */
            if (isHasMoreStreamTable(sourceData, config) && select.getJoins() == null) {
                return;
            }
            /*
             * 窗口类型=有窗口，必须有group by 或者 纯聚合的select
             */
            if (CollectionUtils.isNotEmpty(select.getGroupByColumnReferences())) {
                return;
            }
            List<SelectItem> selectItems = select.getSelectItems();
            for (SelectItem selectItem : selectItems) {
                if (selectItem instanceof SelectExpressionItem) {
                    Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                    if (!aggrExpDetector.isAggregationExpression(expression)) {
                        throw new FailedOnDeParserException("selected.window.type.sql.error",
                                new Object[0], FlinkSqlDeParser.class);
                    }
                } else {
                    throw new FailedOnDeParserException("selected.window.type.sql.error",
                            new Object[0], FlinkSqlDeParser.class);
                }
            }
        } else {
            /*
             * 窗口类型=无窗口，不能有聚合函数和group by
             */
            if (CollectionUtils.isNotEmpty(select.getGroupByColumnReferences())) {
                throw new FailedOnDeParserException("no.window.type.sql.error",
                        new Object[0], FlinkSqlDeParser.class);
            }
            List<SelectItem> selectItems = select.getSelectItems();
            for (SelectItem selectItem : selectItems) {
                if (selectItem instanceof SelectExpressionItem) {
                    Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                    if (aggrExpDetector.isAggregationExpression(expression)) {
                        throw new FailedOnDeParserException("no.window.type.sql.error",
                                new Object[0], FlinkSqlDeParser.class);
                    }
                }
            }
        }
    }

    public boolean isStaticTable(String tableName, Config config) {
        if (!config.hasPath("static_data")) {
            return false;
        }
        return ImmutableSet.copyOf(config.getStringList("static_data")).contains(tableName);
    }

    /**
     * 检查配置的数据源表是否报存在多个实时数据源
     *
     * @return true if has more stream table
     */
    public boolean isHasMoreStreamTable(List<String> sourceData, Config config) {
        long streamTableTotal = sourceData.stream()
                .filter(table -> !isStaticTable(table, config))
                .count();
        if (streamTableTotal >= 2) {
            return true;
        }
        return false;
    }

    /**
     * 窗口类型=有窗口，不能含有静态关联
     * 窗口类型=无窗口，不能含有两个实时数据源表
     */
    public void checkIsLegalStaticWindowType(List<String> sourceData, Config config) {
        String windowType = config.hasPath(PROPERTY_KEY_WINDOW_TYPE)
                ? config.getString(PROPERTY_KEY_WINDOW_TYPE) : null;
        if (TUMBLING_WINDOW.equalsIgnoreCase(windowType)
                || SLIDING_WINDOW.equalsIgnoreCase(windowType)
                || SESSION_WINDOW.equalsIgnoreCase(windowType)
                || ACCUMULATE_WINDOW.equalsIgnoreCase(windowType)) {
            if (config.hasPath(PROPERTY_KEY_IS_STATIC_DATA)
                    && config.getStringList(PROPERTY_KEY_IS_STATIC_DATA) != null
                    && config.getStringList(PROPERTY_KEY_IS_STATIC_DATA).size() > 0) {
                throw new FailedOnDeParserException("static.join.window.type.error",
                        new Object[0], FlinkSqlDeParser.class);
            }
        } else if (isHasMoreStreamTable(sourceData, config)) {
            throw new FailedOnDeParserException("only.tumbling.window.for.stream.join.error",
                    new Object[0], FlinkSqlDeParser.class);
        }
    }

    public void checkColumnNameTypo(Set<String> columns, String columnName) {
        for (String originColumn : columns) {
            if (originColumn.equalsIgnoreCase(columnName)) {
                throw new FailedOnDeParserException("column.typo.error",
                        new Object[]{columnName, originColumn}, FlinkSqlDeParser.class);
            }
        }
        throw new FailedOnDeParserException("column.not.found.error",
                new Object[]{columnName}, FlinkSqlDeParser.class);
    }

    public void checkColumnTypo(Set<String> columns, Column column) {
        for (String originColumn : columns) {
            if (originColumn.equalsIgnoreCase(FlinkSqlDeParser.getColumnName(column))) {
                throw new FailedOnDeParserException("column.typo.error",
                        new Object[]{FlinkSqlDeParser.getColumnName(column), originColumn}, FlinkSqlDeParser.class);
            }
        }
    }

    /**
     * 校验 列名是否在所有表中不存在或者存在于多个静态表中
     *
     * @param isColumnInFactTable 左边的事实表schema是否包含指定列名
     * @param columnName 指定列名
     * @param factTableName 事实表名
     * @param factSchema 事实表schema
     * @param staticTableSchema 静态表名及其schema
     */
    public void checkSelectColumnInTable(boolean isColumnInFactTable,
                                         String columnName,
                                         String factTableName,
                                         GeneratedSchema factSchema,
                                         Map<String, GeneratedSchema> staticTableSchema) {
        // check column not exist in any table
        if (!isColumnInFactTable && staticTableSchema.size() == 0) {
            // 忽略事实表字段名称大小进行检查
            checkColumnNameTypo(factSchema.asMap().keySet(), columnName);
        }
        // check column in duplicated table (fact table and static table)
        if (isColumnInFactTable && staticTableSchema.size() >= 1) {
            throw new FailedOnDeParserException("column.duplicated.fact.static.table.error",
                    new Object[]{columnName, factTableName,
                            staticTableSchema.keySet().toString()}, FlinkSqlDeParser.class);
        }
        // check column in duplicated table (more than two static table)
        if (staticTableSchema.size() >= 2) {
            throw new FailedOnDeParserException("column.duplicated.static.table.error",
                    new Object[]{columnName, staticTableSchema.keySet().toString()}, FlinkSqlDeParser.class);
        }
    }

    /**
     * 1. JOIN左表不能为静态表
     * 2. 超过1个以上的多Join语句必须右表都是静态表
     * 3. JOIN右表如果是子查询，子查询内不能是静态表
     *
     * @param select select clause
     */
    public void checkJoinClauses(PlainSelect select, Config config) {
        FromItem fromItem = select.getFromItem();
        if (fromItem instanceof Table) {
            Table tableLeft = (Table) fromItem;
            String leftTableName = FlinkSqlDeParser.getTableName(tableLeft);
            if (isStaticTable(leftTableName, config)) {
                throw new FailedOnDeParserException("static.join.left.table.error",
                        new Object[0], FlinkSqlDeParser.class);
            }
        }

        int joinClauseTotal = select.getJoins().size();
        // right subquery not support query from static table
        if (joinClauseTotal == 1) {
            Join join = select.getJoins().get(0);
            if (join.getRightItem() instanceof SubSelect) {
                PlainSelect subSelect = (PlainSelect) ((SubSelect) join.getRightItem()).getSelectBody();
                if (subSelect.getFromItem() instanceof Table) {
                    String subTableName = ((Table) (subSelect.getFromItem())).getName();
                    if (isStaticTable(subTableName, config)) {
                        throw new FailedOnDeParserException("static.join.right.sub.query.error",
                                new Object[0], FlinkSqlDeParser.class);
                    }
                }
            }
        }
        //multi right join must be static table at all.
        if (joinClauseTotal >= 2) {
            long staticTableTotal = select.getJoins().stream()
                    .filter(join -> (join.getRightItem() instanceof Table))
                    .map(join -> ((Table) join.getRightItem()).getName())
                    .filter(tableName -> isStaticTable(tableName, config))
                    .count();
            if (joinClauseTotal != staticTableTotal) {
                throw new FailedOnDeParserException("static.join.right.table.error",
                        new Object[0], FlinkSqlDeParser.class);
            }
        }
    }

    /**
     * 检查配置实时数据源表数量和SQL使用的数据源表数量是否一致
     *
     * @param parents SQL使用的数据源表
     * @throws FailedOnDeParserException 上游节点表没有被使用
     */
    public void checkSourceTableUse(Set<String> parents, List<String> sourceData, Config config) {
        long genTableTotal = parents.stream()
                .filter(table -> table.contains(GENERATE_FLINKSQL_STATIC_JOIN_TABLE_SUFFIX)
                        || table.contains(GENERATE_FLINKSQL_JOIN_TABLE_SUFFIX)
                        || table.contains(GENERATE_FLINKSQL_SUBQUERY_TABLE_SUFFIX))
                .count();
        if (genTableTotal > 0) {
            return;
        }
        String[] notUseTableArray = sourceData.stream()
                .filter(table -> !isStaticTable(table, config))
                .filter(table -> !parents.contains(table))
                .toArray(String[]::new);
        if (notUseTableArray.length > 0) {
            throw new FailedOnDeParserException("join.table.not.use.error",
                    new Object[]{Arrays.toString(notUseTableArray)}, FlinkSqlDeParser.class);
        }
    }

    /**
     * 检查sql中表名和输入配置是否一致
     *
     * @param tableName 表名
     */
    public void checkInputTable(String tableName, List<String> sourceData) {
        if (!sourceData.isEmpty() && !sourceData.contains(tableName)) {
            throw new FailedOnDeParserException("from.table.error",
                    new Object[]{tableName}, FlinkSqlDeParser.class);
        }
    }

    /**
     * 静态表中的key必须全部进行关联
     *
     * @param sortedJoinKeys 生成的key
     * @param staticTableName 静态表名称
     * @param staticTableKeys 静态关联Key
     */
    public void checkStaticKey(List<Map<String, String>> sortedJoinKeys,
                               String staticTableName,
                               List<String> staticTableKeys) {
        if (sortedJoinKeys.contains(null)) {
            throw new FailedOnDeParserException("static.join.no.key.error",
                    new Object[]{staticTableName, staticTableKeys.toString()}, FlinkSqlDeParser.class);
        }
    }
}
