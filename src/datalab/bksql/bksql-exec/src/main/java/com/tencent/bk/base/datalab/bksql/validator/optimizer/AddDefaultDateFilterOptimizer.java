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

import static com.tencent.bk.base.datalab.bksql.enums.BkDataReservedFieldEnum.THEDATE;

import com.google.common.collect.ImmutableSet;
import com.tencent.bk.base.datalab.bksql.util.QuoteUtil;
import com.tencent.bk.base.datalab.bksql.util.calcite.visitor.ExpressionVisitorAdapter;
import com.tencent.bk.base.datalab.bksql.validator.SimpleListenerWalker;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class AddDefaultDateFilterOptimizer extends SimpleListenerWalker {

    private static final Set<String> TIME_BASED_FILTER_NAMES = ImmutableSet
            .of("dteventtimestamp", "dteventtime", "thedate");

    private static final String PATTERN = "yyyyMMdd";
    private static final String BK_TIMEZONE = "BK_TIMEZONE";
    private static final String ASIA = "Asia/Shanghai";

    @Override
    public void enterQueryNode(SqlCall call) {
        super.enterQueryNode(call);
        resolveQueryNode(call);
    }

    /**
     * 外层查询语句处理逻辑
     *
     * @param call 外层查询语句
     */
    private void resolveQueryNode(SqlCall call) {
        if (call instanceof SqlSelect) {
            addDefaultDateFilter((SqlSelect) call);
        } else if (call instanceof SqlOrderBy) {
            SqlNode query = ((SqlOrderBy) call).query;
            if (query instanceof SqlSelect) {
                addDefaultDateFilter((SqlSelect) query);
            } else {
                resolveQueryNode((SqlCall) query);
            }
        }
    }

    /**
     * 添加默认日期过滤条件
     *
     * @param select SqlSelect节点
     */
    private void addDefaultDateFilter(SqlSelect select) {
        SqlSelect plainSelect = select;
        SqlNode where = plainSelect.getWhere();
        SqlNode whereReplace;
        if (noNestingOrJoinIncluded(plainSelect)) {
            SqlNode[] operands = new SqlNode[2];
            operands[0] = new SqlIdentifier(THEDATE.name().toLowerCase(),
                    plainSelect.getParserPosition());
            operands[1] = SqlLiteral.createCharString(today(), plainSelect.getParserPosition());
            SqlNode defaultFilter = new SqlBasicCall(SqlStdOperatorTable.EQUALS, operands,
                    plainSelect.getParserPosition());
            if (where == null) {
                whereReplace = defaultFilter;
            } else if (noTimeBaseFilterIncluded(where)) {
                SqlNode[] andOperands = new SqlNode[2];
                andOperands[0] = defaultFilter;
                andOperands[1] = where;
                whereReplace = new SqlBasicCall(SqlStdOperatorTable.AND, andOperands,
                        plainSelect.getParserPosition());
            } else {
                whereReplace = where;
            }
            plainSelect.setWhere(whereReplace);
        }
    }

    /**
     * 获取今天日期字符串 yyyyMMdd格式
     *
     * @return
     */
    private String today() {
        ZoneId zoneId = ZoneId.of(System.getProperty(BK_TIMEZONE, ASIA));
        return ZonedDateTime.now(zoneId)
                .format(DateTimeFormatter.ofPattern(PATTERN));
    }

    /**
     * 校验是否存在日期过滤条件
     *
     * @param where where字句
     * @return 是否存在日期过滤条件
     */
    private boolean noTimeBaseFilterIncluded(SqlNode where) {
        boolean[] noTimeBaseFilterIncluded = new boolean[]{true};
        where.accept(new ExpressionVisitorAdapter() {
            @Override
            public Void visit(SqlIdentifier column) {
                super.visit(column);
                if (column == null) {
                    return null;
                }
                if (TIME_BASED_FILTER_NAMES
                        .contains(QuoteUtil.trimQuotes(column.names.get(column.names.size() - 1))
                                .toLowerCase())) {
                    noTimeBaseFilterIncluded[0] = false;
                }
                return null;
            }
        });
        return noTimeBaseFilterIncluded[0];
    }

    /**
     * 校验是否是子查询或者join查询
     *
     * @param plainSelect SqlSelect节点
     * @return 是否是子查询或者join查询
     */
    private boolean noNestingOrJoinIncluded(SqlSelect plainSelect) {
        boolean result = false;
        SqlNode fromNode = plainSelect.getFrom();
        if (fromNode instanceof SqlIdentifier) {
            result = true;
        }
        if (fromNode instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) fromNode;
            if (call.getOperator() instanceof SqlAsOperator) {
                SqlNode beforeAs = call.operand(0);
                if (beforeAs instanceof SqlIdentifier) {
                    result = true;
                }
            }
        }
        return result;
    }
}
