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

package com.tencent.bk.base.dataflow.bksql.validator.optimizer;

import com.tencent.blueking.bksql.exception.FailedOnCheckException;
import com.tencent.blueking.bksql.validator.ExpressionReplacementOptimizer;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.TableFunction;

import java.util.ArrayList;
import java.util.List;

public class SparkSqlLateralTableOptimizer extends ExpressionReplacementOptimizer {

    /**
     * 进行lateral table语句转换为spark sql
     * <code>SELECT a as a from t, lateral table(parseUdtf(content)) as T(a, b, c)</code>
     * 转换为LATERAL VIEW 写法
     * <code>SELECT a AS a FROM t LATERAL VIEW parseUdtf(content) T AS a,b,c</code>
     *
     * @param select
     */
    @Override
    public void enterNode(PlainSelect select) {

        List<Join> joins = select.getJoins();
        if (null != joins) {
            List<Join> newJoins = new ArrayList<>();
            for (Join join : joins) {
                Join newJoin = join;
                FromItem rightItem = join.getRightItem();
                if (rightItem instanceof TableFunction) {
                    TableFunction tf = (TableFunction) rightItem;
                    if (tf.isLiteral()) {
                        newJoin = this.convertToLateralTable(join, tf);
                    }
                }
                newJoins.add(newJoin);
            }
            select.setJoins(newJoins);
        }
    }

    private Join convertToLateralTable(Join join, TableFunction tf) {
        Join newJoin;
        boolean isLeftJoinOnTrue = this.checkIsLeftJoinOnTrue(join);
        boolean isCrossJoin = this.checkIsCrossJoin(join);

        if (join.isSimple() || isLeftJoinOnTrue || isCrossJoin) {
            SparkLateralTableFunction sparkTableFunction = new SparkLateralTableFunction();
            sparkTableFunction.setAlias(
                    new SparkLateralTableAlias(tf.getAlias().getName(), tf.getAlias().isUseAs()));
            sparkTableFunction.setFunction(tf.getFunction());
            sparkTableFunction.setLiteral(tf.isLiteral());
            sparkTableFunction.setPivot(tf.getPivot());
            sparkTableFunction.setOuter(isLeftJoinOnTrue);
            //
            newJoin = new SparkLateralTableJoin();
            newJoin.setSimple(false);
            newJoin.setRightItem(sparkTableFunction);
            newJoin.setOnExpression(join.getOnExpression());
            newJoin.setUsingColumns(join.getUsingColumns());
        } else {
            throw new FailedOnCheckException("lateral.table.only.cross.and.left.join", new Object[]{},
                    SparkSqlLateralTableOptimizer.class);
        }
        return newJoin;
    }

    private boolean checkIsLeftJoinOnTrue(Join join) {
        boolean isLeftJoinOnTrue = false;
        if (!join.isSimple()) {
            if (join.isLeft()
                    && null != join.getOnExpression()
                    && join.getOnExpression() instanceof BooleanValue) {
                BooleanValue booleanValue = (BooleanValue) join.getOnExpression();
                if (booleanValue.getValue()) {
                    isLeftJoinOnTrue = true;
                }
            }
        }
        return isLeftJoinOnTrue;
    }

    private boolean checkIsCrossJoin(Join join) {
        boolean isCrossJoin = false;
        if (!join.isSimple()) {
            if (join.isCross()) {
                isCrossJoin = true;
                if (null != join.getOnExpression()) {
                    if (join.getOnExpression() instanceof BooleanValue && (((BooleanValue) join
                            .getOnExpression()).getValue())) {
                        // OK
                    } else {
                        throw new FailedOnCheckException("lateral.table.only.cross.and.left.join",
                                new Object[]{}, SparkSqlLateralTableOptimizer.class);
                    }
                } else {
                    // OK
                }
            }
        }
        return isCrossJoin;
    }

    public static class SparkLateralTableJoin extends Join {

        @Override
        public String toString() {
            return "" + this.getRightItem();
        }
    }

    public static class SparkLateralTableFunction extends TableFunction {

        private boolean isOuter = false;

        public boolean isOuter() {
            return isOuter;
        }

        public void setOuter(boolean outer) {
            isOuter = outer;
        }

        @Override
        public String toString() {
            if (this.isLiteral()) {
                return "LATERAL VIEW " + (isOuter ? "OUTER " : "") + getFunction() + "" + ((getAlias() != null)
                        ? getAlias().toString() : "");
            }
            return super.toString();
        }
    }

    public static class SparkLateralTableAlias extends Alias {

        public SparkLateralTableAlias(String name, boolean useAs) {
            super(name, useAs);
        }

        @Override
        public String toString() {
            String[] cols = this.getName().split("\\(", 2);
            String lTab = cols[0];
            String columnNames = cols[1].substring(0, cols[1].length() - 1);
            return " " + lTab + " AS " + columnNames;
        }
    }
}
