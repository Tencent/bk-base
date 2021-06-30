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

import com.tencent.blueking.bksql.exception.MessageLocalizedException;
import com.tencent.blueking.bksql.validator.ExpressionReplacementOptimizer;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.TableFunction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SparkSqlIpLibOptimizer extends ExpressionReplacementOptimizer {

    public static final ThreadLocal<HashSet<String>> USED_IP_LIB_HASH_SET = ThreadLocal.<HashSet<String>>withInitial(
            () -> new HashSet<>());

    public static final IpLibMeta[] IP_LIB_METAS = {
            new IpLibMeta(
                    "iplib_591", //ipv4_gslb_591
                    "ipv4link_udtf",
                    new String[]{
                            "ip",
                            "country",
                            "province",
                            "city",
                            "region",
                            "front_isp",
                            "backbone_isp",
                            "asid",
                            "comment"},
                    "ip"

            ),
            new IpLibMeta(
                    "ipv6lib_591", //ipv6_gslb_591
                    "ipv6link_udtf",
                    new String[]{
                            "ipv6",
                            "country",
                            "province",
                            "city",
                            "region",
                            "isp",
                            "asname",
                            "asid",
                            "comment"},
                    "ipv6"

            ),
            new IpLibMeta(
                    "ipv4_gslb_591",
                    "ipv4link_udtf",
                    new String[]{
                            "ip",
                            "country",
                            "province",
                            "city",
                            "region",
                            "front_isp",
                            "backbone_isp",
                            "asid",
                            "comment"},
                    "ip"

            ),
            new IpLibMeta(
                    "ipv6_gslb_591",
                    "ipv6link_udtf",
                    new String[]{
                            "ipv6",
                            "country",
                            "province",
                            "city",
                            "region",
                            "isp",
                            "asname",
                            "asid",
                            "comment"},
                    "ipv6"

            ),
            new IpLibMeta(
                    "ipv4_tgeo_generic_business_591",
                    "ipv4linkTgeoGenericBusiness_udtf",
                    new String[]{
                            "ip",
                            "country",
                            "province",
                            "city",
                            "region",
                            "front_isp",
                            "backbone_isp",
                            "asid",
                            "comment"},
                    "ip"

            ),
            new IpLibMeta(
                    "ipv6_tgeo_generic_business_591",
                    "ipv6linkTgeoGenericBusiness_udtf",
                    new String[]{
                            "ipv6",
                            "country",
                            "province",
                            "city",
                            "region",
                            "isp",
                            "asname",
                            "asid",
                            "comment"},
                    "ipv6"

            ),
            new IpLibMeta(
                    "ipv4_tgeo_base_network_591",
                    "ipv4linkTgeoBaseNetwork_udtf",
                    new String[]{
                            "ip",
                            "country",
                            "province",
                            "city",
                            "region",
                            "front_isp",
                            "backbone_isp",
                            "asid",
                            "comment"},
                    "ip"

            ),
            new IpLibMeta(
                    "ipv6_tgeo_base_network_591",
                    "ipv6linkTgeoBaseNetwork_udtf",
                    new String[]{
                            "ipv6",
                            "country",
                            "province",
                            "city",
                            "region",
                            "isp",
                            "asname",
                            "asid",
                            "comment"},
                    "ipv6"

            )
    };

    @Override
    public void enterNode(PlainSelect select) {
        USED_IP_LIB_HASH_SET.remove();
        for (IpLibMeta ipLibMeta : IP_LIB_METAS) {
            tryToConvertIpLib(ipLibMeta, select);
        }
    }

    private void tryToConvertIpLib(IpLibMeta ipLibMeta, PlainSelect select) {
        IpLibUdtfConverter ipLibConverter = new IpLibUdtfConverter(ipLibMeta, select);
        select.setJoins(ipLibConverter.generateJoinListWithIpLib());
        if (ipLibConverter.isIpLibConverted()) {
            USED_IP_LIB_HASH_SET.get().add(ipLibMeta.getLogicalIpTableName());
        }
    }

    public static class IpLibUdtfConverter {

        private IpLibMeta meta;
        private PlainSelect select;
        private boolean convertedIpLib = false;

        public IpLibUdtfConverter(IpLibMeta ipLibMeta, PlainSelect select) {
            this.meta = ipLibMeta;
            this.select = select;
        }

        private List<Join> generateJoinListWithIpLib() {
            Join ipJoin = null;
            String currentIpLibName = null;
            FromItem fromItem = this.select.getFromItem();
            if (fromItem instanceof Table) {
                if (((Table) fromItem).getName().equals(this.meta.getLogicalIpTableName())) {
                    ipJoin = checkIpLibInFromItem(this.select, this.meta);
                    currentIpLibName = ipJoin.getRightItem().getAlias().getName();
                    convertedIpLib = true;
                }
            }

            List<Join> finalJoinList = new ArrayList<>();

            List<Join> joinList = this.select.getJoins();
            if (null != joinList) {
                for (Join joinItem : joinList) {
                    FromItem rightItem = joinItem.getRightItem();
                    if (rightItem instanceof Table) {
                        checkIpLibUsedInUnsupportedJoin(joinItem.getOnExpression(), currentIpLibName);
                        if (((Table) rightItem).getName().equals(this.meta.getLogicalIpTableName())) {
                            if (ipJoin != null) {
                                throw new MessageLocalizedException(
                                        "iplinklibrary.join.ipv4.only.once", null, SparkSqlIpLibOptimizer.class);
                            }
                            ipJoin = checkIpLibInJoinItem(joinItem, this.meta);
                            currentIpLibName = ipJoin.getRightItem().getAlias().getName();
                            convertedIpLib = true;
                        } else {
                            finalJoinList.add(joinItem);
                        }
                    } else {
                        finalJoinList.add(joinItem);
                    }
                }
            }
            if (ipJoin != null) {
                finalJoinList.add(ipJoin);
            }
            return finalJoinList;
        }

        private boolean isIpLibConverted() {
            return this.convertedIpLib;
        }

        private Join checkIpLibInFromItem(PlainSelect select, IpLibMeta ipLibMeta) {
            List<Join> joinList = select.getJoins();
            Table fromItem = (Table) select.getFromItem();

            String ipLibName = getIpLibName(fromItem);

            Join resultJoin;
            if (null != joinList) {
                Join joinItem = joinList.remove(0);
                if (joinItem.isInner()) {
                    resultJoin = getIpJoinItem(joinItem, ipLibName, ipLibMeta, false);
                } else if (joinItem.isRight()) {
                    resultJoin = getIpJoinItem(joinItem, ipLibName, ipLibMeta, true);
                } else {
                    throw new MessageLocalizedException("iplinklibrary.inner.left.join", null,
                            SparkSqlIpLibOptimizer.class);
                }

                select.setFromItem(joinItem.getRightItem());
                select.setJoins(joinList);
                return resultJoin;
            } else {
                throw new MessageLocalizedException("iplinklibrary.inner.left.join", null,
                        SparkSqlIpLibOptimizer.class);
            }
        }

        private Join checkIpLibInJoinItem(Join joinItem, IpLibMeta ipLibMeta) {
            Table rightItem = (Table) joinItem.getRightItem();
            String ipLibName = getIpLibName(rightItem);
            if (joinItem.isInner()) {
                return getIpJoinItem(joinItem, ipLibName, ipLibMeta, false);
            } else if (joinItem.isLeft()) {
                return getIpJoinItem(joinItem, ipLibName, ipLibMeta, true);
            } else {
                throw new MessageLocalizedException("iplinklibrary.inner.left.join", null,
                        SparkSqlIpLibOptimizer.class);
            }
        }

        private void checkIpLibUsedInUnsupportedJoin(Expression expression, String ipTableName) {
            if (ipTableName != null) {
                checkExpressionType(expression, "equals_to");
                EqualsTo equalsToExpression = (EqualsTo) expression;
                checkExpressionType(equalsToExpression.getLeftExpression(), "column");
                Column leftColumn = (Column) equalsToExpression.getLeftExpression();
                checkExpressionType(equalsToExpression.getRightExpression(), "column");
                Column rightColumn = (Column) equalsToExpression.getRightExpression();

                if (rightColumn.getTable().getName().equals(ipTableName) || leftColumn.getTable().getName()
                        .equals(ipTableName)) {
                    throw new MessageLocalizedException("iplinklibrary.only.on.direct.join", null,
                            SparkSqlIpLibOptimizer.class);
                }
            }
        }

        private Join getIpJoinItem(Join oldJoinItem, String ipLibName, IpLibMeta ipLibMeta,
                boolean isLateralViewOuter) {
            String columnName = retreiveIpColumnFromEqualsTo(
                    oldJoinItem.getOnExpression(), ipLibName, ipLibMeta.getIpColumn());

            return createIpLibJoin(
                    columnName,
                    isLateralViewOuter,
                    ipLibMeta,
                    ipLibName);
        }

        private String getIpLibName(Table table) {
            if (table.getAlias() != null && table.getAlias().getName() != null) {
                return table.getAlias().getName();
            }
            return table.getName();
        }

        private void checkExpressionType(Expression expression, String type) {
            if (!expression.getExpressionType().equals(type)) {
                throw new MessageLocalizedException("iplinklibrary.join.expression.fault", null,
                        SparkSqlIpLibOptimizer.class);
            }
        }

        private String retreiveIpColumnFromEqualsTo(Expression expression, String ipTableName, String iplibIpColumn) {
            Expression onExpression = expression;
            if (expression.getExpressionType().equals("parenthesis")) {
                onExpression = ((Parenthesis) expression).getExpression();
            }
            checkExpressionType(onExpression, "equals_to");
            EqualsTo equalsToExpression = (EqualsTo) onExpression;
            checkExpressionType(equalsToExpression.getLeftExpression(), "column");
            Column leftColumn = (Column) equalsToExpression.getLeftExpression();
            checkExpressionType(equalsToExpression.getRightExpression(), "column");
            Column rightColumn = (Column) equalsToExpression.getRightExpression();

            if (!rightColumn.getTable().getName().equals(leftColumn.getTable().getName())) {
                if (rightColumn.getTable().getName().equals(ipTableName)
                        && rightColumn.getColumnName().toLowerCase().equals(iplibIpColumn.toLowerCase())) {
                    return leftColumn.toString();
                } else if (leftColumn.getTable().getName().equals(ipTableName)
                        && leftColumn.getColumnName().toLowerCase().equals(iplibIpColumn.toLowerCase())) {
                    return rightColumn.toString();
                }
            }
            throw new MessageLocalizedException("iplinklibrary.join.expression.fault", null,
                    SparkSqlIpLibOptimizer.class);
        }

        private Join createIpLibJoin(
                String ipColName,
                boolean isLateralViewOuter,
                IpLibMeta ipLibMeta,
                String ipLibName) {
            SparkIpLibJoin ipJoin = new SparkIpLibJoin();
            ipJoin.setRightItem(createSparkIpLibTableFunction(ipColName, isLateralViewOuter, ipLibMeta, ipLibName));
            return ipJoin;
        }

        private SparkIpLibTableFunction createSparkIpLibTableFunction(
                String ipColName,
                boolean isLateralViewOuter,
                IpLibMeta ipLibMeta,
                String ipLibname) {
            String udtfName = ipLibMeta.getIpLibUdtfName();

            Function ipLibFunction = new Function();
            ipLibFunction.setName(udtfName);

            ArrayList<Expression> expressionList = new ArrayList();

            Column ipColumn = new Column();
            ipColumn.setColumnName(ipColName);
            expressionList.add(ipColumn);

            Column modeColumn = new Column();
            String mode = isLateralViewOuter ? "'outer'" : "'no_outer'";
            modeColumn.setColumnName(mode);
            expressionList.add(modeColumn);

            ExpressionList parameters = new ExpressionList();
            parameters.setExpressions(expressionList);
            ipLibFunction.setParameters(parameters);

            String[] udtfHeaders = ipLibMeta.getHeaders();
            SparkIpLibTableFunction sparkIpLibTableFunction = new SparkIpLibTableFunction();
            sparkIpLibTableFunction.setAlias(new SparkIpLibTableAlias(ipLibname, udtfHeaders));
            sparkIpLibTableFunction.setFunction(ipLibFunction);
            sparkIpLibTableFunction.setLiteral(true);
            sparkIpLibTableFunction.setOuter(isLateralViewOuter);

            return sparkIpLibTableFunction;
        }
    }


    public static class IpLibMeta {

        private String logicalIpTableName;
        private String ipLibUdtfName;
        private String[] headers;
        private String ipColumn;

        public IpLibMeta(String logicalIpTableName, String ipLibUdtfName, String[] headers, String ipColumn) {
            this.logicalIpTableName = logicalIpTableName;
            this.ipLibUdtfName = ipLibUdtfName;
            this.headers = headers;
            this.ipColumn = ipColumn;
        }

        public String getLogicalIpTableName() {
            return logicalIpTableName;
        }

        public String getIpLibUdtfName() {
            return ipLibUdtfName;
        }

        public String[] getHeaders() {
            return headers;
        }

        public String getIpColumn() {
            return ipColumn;
        }
    }

    public static class SparkIpLibJoin extends Join {

        @Override
        public String toString() {
            return "" + this.getRightItem();
        }
    }

    public static class SparkIpLibTableFunction extends TableFunction {

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

    public static class SparkIpLibTableAlias extends Alias {

        private String[] headers;

        public SparkIpLibTableAlias(String name, String[] headers) {
            super(name, true);
            this.headers = headers;
        }

        @Override
        public String toString() {
            StringBuilder strBuider = new StringBuilder("");
            strBuider.append(" ");
            strBuider.append(this.getName());
            strBuider.append(" AS ");
            for (String header : this.headers) {
                strBuider.append(header);
                strBuider.append(",");
            }
            strBuider.deleteCharAt(strBuider.lastIndexOf(","));
            return strBuider.toString();
        }
    }
}
