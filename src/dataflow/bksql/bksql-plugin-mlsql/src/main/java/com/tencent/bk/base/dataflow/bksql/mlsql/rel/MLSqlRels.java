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

package com.tencent.bk.base.dataflow.bksql.mlsql.rel;

import com.google.common.collect.ImmutableSet;
import com.tencent.bk.base.datalab.bksql.exception.MessageLocalizedException;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

public final class MLSqlRels {

    private MLSqlRels() {

    }

    public static List<String> mlsqlFieldNames(final RelDataType rowType) {
        return SqlValidatorUtil.uniquify(
                rowType.getFieldList()
                        .stream()
                        .map(RelDataTypeField::getName)
                        .collect(Collectors.toList()),
                SqlValidatorUtil.EXPR_SUGGESTER, true);
    }

    public static String generateTableName(MLSqlTaskContext context, MLSqlRel rel) {
        if (context.isRoot(rel)) {
            return context.getTableName();
        }
        return String.format("%s_%s_%d", context.getTableName(), rel.getRelTypeName(), context.getNextID());
    }

    public static String toMLSqlType(RelDataType fromType) {
        return toMLSqlType(fromType.getSqlTypeName());
    }

    /**
     * 将常规sql类型转换为MLSQL内支持的数据类型
     *
     * @param sqlTypeName 常规sql类型名称
     * @return 转换后的类型名称
     */
    public static String toMLSqlType(SqlTypeName sqlTypeName) {
        switch (sqlTypeName) {
            case SMALLINT:
            case TINYINT:
            case INTEGER:
                return "int";
            case BIGINT:
                return "long";
            case FLOAT:
                return "float";
            case DOUBLE:
            case DECIMAL:
                return "double";
            case CHAR:
            case VARCHAR:
            case ANY:
                return "string";
            case TIME:
            case TIMESTAMP:
                return "timestamp";
            case BOOLEAN:
                return "boolean";
            default:
                throw new IllegalArgumentException("unexpected sql type: " + sqlTypeName);
        }
    }

    /**
     * Code generator of Storm SQL.
     */
    public static class RexToMLSqlTranslator extends SimpleRexVisitorImpl<String> {

        private static final List<SqlTypeName> CHARACTER_TYPES = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.CHAR);

        private final List<String> fieldNames;
        private final JavaTypeFactory typeFactory;
        private final MLSqlRexCallImplTable callImplTable;

        protected RexToMLSqlTranslator(List<String> fieldNames, JavaTypeFactory typeFactory) {
            super();
            this.fieldNames = fieldNames;
            this.typeFactory = typeFactory;
            callImplTable = initCallImplTable();
        }

        /**
         * Supported
         * -----------------------------------------------------------------
         * +, AND, OR, =, >=, >, <=, <, -, *, /, IS NOT NULL, IS NULL, LIKE,
         * NOT,CHAR_LENGTH, ROUND, CAST, POWER, ABS, SQRT, FLOOR, CEIL, TRUNCATE,
         * SUBSTRING, UPPER, LOWER, TRIM, MOD, REPLACE
         * LEFT, RIGHT, REGEXP_REPLACE, CONCAT, SUBSTRING_INDEX, UNIXTIME_DIFF,
         * LTRIM, RTRIM, LOCATE, MID, IF, SPLIT_INDEX, INET_ATON, CONCAT_WS,
         * NOW, INET_NTOA, CONTAINS_SUBSTRING, UNIX_TIMESTAMP, CURDATE,
         * FROM_UNIXTIME, INSTR, INT_OVERFLOW, ADD_ONLINECNT
         * -----------------------------------------------------------------
         *
         * @return Expression code
         */
        private MLSqlRexCallImplTable initCallImplTable() {
            MLSqlRexCallImplTable implTable = new MLSqlRexCallImplTable();

            implTable.registerImpl(SqlStdOperatorTable.AND, translateAndOr("&&"));
            implTable.registerImpl(SqlStdOperatorTable.OR, translateAndOr("||"));

            // comparison operator
            implTable.registerImpl(SqlStdOperatorTable.EQUALS, translateComparison("=="));
            implTable.registerImpl(SqlStdOperatorTable.NOT_EQUALS, translateComparison("!="));
            implTable.registerImpl(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, translateComparison(">="));
            implTable.registerImpl(SqlStdOperatorTable.GREATER_THAN, translateComparison(">"));
            implTable.registerImpl(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, translateComparison("<="));
            implTable.registerImpl(SqlStdOperatorTable.LESS_THAN, translateComparison("<"));

            implTable.registerImpl(SqlStdOperatorTable.PLUS, translateArithmetic("+"));
            implTable.registerImpl(SqlStdOperatorTable.MINUS, translateArithmetic("-"));
            implTable.registerImpl(SqlStdOperatorTable.MULTIPLY, translateArithmetic("*"));
            //implTable.registerImpl(MLSqlOperatorTable.DIVIDE, translateDivide("/"));

            implTable.registerImpl(SqlStdOperatorTable.IS_NOT_NULL, translateNullPredicate("!= null"));
            implTable.registerImpl(SqlStdOperatorTable.IS_NULL, translateNullPredicate("== null"));

            implTable.registerImpl(SqlStdOperatorTable.LIKE, translateLike());

            implTable.registerImpl(SqlStdOperatorTable.NOT, translateNot());

            // calcite system function
            implTable.registerImpl(SqlStdOperatorTable.CHAR_LENGTH, translateCharLength());
            //implTable.registerImpl(MLSqlOperatorTable.ROUND, translateRound());
            implTable.registerImpl(SqlStdOperatorTable.CAST, translateCast());
            implTable.registerImpl(SqlStdOperatorTable.POWER, translatePower());
            implTable.registerImpl(SqlStdOperatorTable.ABS, translateAbs());
            implTable.registerImpl(SqlStdOperatorTable.SQRT, translateSqrt());
            implTable.registerImpl(SqlStdOperatorTable.FLOOR, translateFloor());
            implTable.registerImpl(SqlStdOperatorTable.CEIL, translateCeil());
            implTable.registerImpl(SqlStdOperatorTable.TRUNCATE, translateTruncate());
            implTable.registerImpl(SqlStdOperatorTable.SUBSTRING, translateSubstring());
            implTable.registerImpl(SqlStdOperatorTable.UPPER, translateUpper());
            implTable.registerImpl(SqlStdOperatorTable.LOWER, translateLower());
            implTable.registerImpl(SqlStdOperatorTable.TRIM, translateTrim());
            implTable.registerImpl(SqlStdOperatorTable.MOD, translateMod());
            implTable.registerImpl(SqlStdOperatorTable.REPLACE, translateReplace());
            implTable.registerImpl(SqlStdOperatorTable.CASE, translateCase());

            // custom function
            /*implTable.registerImpl(MLSqlOperatorTable.LEFT, translateLeft());
            implTable.registerImpl(MLSqlOperatorTable.RIGHT, translateRight());
            implTable.registerImpl(MLSqlOperatorTable.REGEXP_REPLACE, translateRegexpReplace());
            implTable.registerImpl(MLSqlOperatorTable.CONCAT, translateConcat());
            implTable.registerImpl(MLSqlOperatorTable.SUBSTRING_INDEX, translateSubstringIndex());
            implTable.registerImpl(MLSqlOperatorTable.UNIXTIME_DIFF, translateUnixtimeDiff());
            implTable.registerImpl(MLSqlOperatorTable.LTRIM, translateLtrim());
            implTable.registerImpl(MLSqlOperatorTable.RTRIM, translateRtrim());
            implTable.registerImpl(MLSqlOperatorTable.LOCATE, translateLocate());
            implTable.registerImpl(MLSqlOperatorTable.MID, translateMid());
            implTable.registerImpl(MLSqlOperatorTable.IF, translateIf());
            implTable.registerImpl(MLSqlOperatorTable.SPLIT_INDEX, translateSplitIndex());
            implTable.registerImpl(MLSqlOperatorTable.INET_ATON, translateInetAton());
            implTable.registerImpl(MLSqlOperatorTable.CONCAT_WS, translateConcatWs());
            implTable.registerImpl(MLSqlOperatorTable.NOW, translateNow());
            implTable.registerImpl(MLSqlOperatorTable.INET_NTOA, translateInetNtoa());
            implTable.registerImpl(MLSqlOperatorTable.CONTAINS_SUBSTRING, translateContainsSubstring());
            implTable.registerImpl(MLSqlOperatorTable.UNIX_TIMESTAMP, translateUnixTimestamp());
            implTable.registerImpl(MLSqlOperatorTable.CURDATE, translateCurDate());
            implTable.registerImpl(MLSqlOperatorTable.FROM_UNIXTIME, translateFromUnixTime());
            implTable.registerImpl(MLSqlOperatorTable.INSTR, translateInstr());
            implTable.registerImpl(MLSqlOperatorTable.INT_OVERFLOW, translateIntOverflow());
            implTable.registerImpl(MLSqlOperatorTable.REGEXP, translateRegexp());
            implTable.registerImpl(MLSqlOperatorTable.REGEXP_EXTRACT, translateregexpExtract());*/

            // todo more
            return implTable;
        }

        @Override
        public String visitInputRef(RexInputRef inputRef) {
            return String.format("${%s}", fieldNames.get(inputRef.getIndex()));
        }

        @Override
        public String visitLiteral(RexLiteral literal) {
            return RexToLixTranslator.translateLiteral(literal,
                    literal.getType(),
                    typeFactory,
                    RexImpTable.NullAs.NULL).toString();
        }

        @Override
        public String visitCall(RexCall call) {
            return callImplTable.implement(call);
        }

        private MLSqlRexCallImplTable.CallImplementor translateArithmetic(String operator) {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                RexNode left = rexNodes.get(0);
                RexNode right = rexNodes.get(1);
                return String.format("(%s %s %s)", translateToPrimitive(left), operator, translateToPrimitive(right));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateDivide(String operator) {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                RexNode left = rexNodes.get(0);
                RexNode right = rexNodes.get(1);
                return String.format("((double) %s %s %s)", translateToPrimitive(left), operator,
                        translateToPrimitive(right));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateBinary(String operator) {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                RexNode left = rexNodes.get(0);
                RexNode right = rexNodes.get(1);
                return String.format("(%s %s %s)", translateToPrimitive(left), operator, translateToPrimitive(right));
            };
        }


        private MLSqlRexCallImplTable.CallImplementor translateAndOr(String operator) {
            return call -> String.format("(%s)", String.join(
                    " " + operator + " ",
                    call.getOperands().stream()
                            .map(this::translateToPrimitive)
                            .collect(Collectors.toList())));
        }

        private MLSqlRexCallImplTable.CallImplementor translateLike() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                RexNode left = rexNodes.get(0);
                RexNode right = rexNodes.get(1);
                String rightRegex = right.toString().replace("'", "\"").replace("%", "(?s).*");
                return String.format("(%s != null && %s.matches(%s))", translateToReference(left),
                        translateToReference(left), rightRegex);
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateComparison(String operator) {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                RexNode left = rexNodes.get(0);
                RexNode right = rexNodes.get(1);
                checkTypeConsistent(left, right, operator);
                if (CHARACTER_TYPES.contains(left.getType().getSqlTypeName()) && CHARACTER_TYPES
                        .contains(right.getType().getSqlTypeName())) {
                    return String.format("(%s != null && %s.compareTo(%s) %s 0)", translateToReference(left),
                            translateToReference(left), translateToReference(right), operator);
                } else {
                    return translateBinary(operator).implement(call);
                }
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateNullPredicate(String operator) {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                RexNode rexNode = rexNodes.get(0);
                return String.format("(%s %s)", translateToReference(rexNode), operator);
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateNot() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                RexNode rexNode = rexNodes.get(0);
                return String.format("!(%s)", translateToReference(rexNode));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateLeft() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                RexNode left = rexNodes.get(0);
                RexNode right = rexNodes.get(1);
                return String.format("(%s).substring(0, %s)",
                        translateToReference(left),
                        translateToPrimitive(right));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateRight() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                RexNode left = rexNodes.get(0);
                RexNode right = rexNodes.get(1);
                return String.format("(%1$s).substring((%1$s).length() - %2$s, (%1$s).length())",
                        translateToReference(left),
                        translateToPrimitive(right));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateRegexpReplace() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3);
                RexNode first = rexNodes.get(0);
                RexNode second = rexNodes.get(1);
                RexNode third = rexNodes.get(2);
                return String
                        .format("(%s).replaceAll(%s, %s)", translateToReference(first), translateToReference(second),
                                translateToReference(third));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateCharLength() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                RexNode rexNode = rexNodes.get(0);
                return String.format("(%s).length()", translateToReference(rexNode));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateRound() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1, 2);
                if (rexNodes.size() == 1) {
                    return String.format("Math.round(%s)", translateToPrimitive(rexNodes.get(0)));
                }
                RexNode left = rexNodes.get(0);
                RexNode right = rexNodes.get(1);
                return String
                        .format("Math.round((%s) * Math.pow((double) 10, (double) %s)) / Math.pow((double) 10, "
                                        + "(double) %s)",
                                translateToPrimitive(left),
                                translateToPrimitive(right),
                                translateToPrimitive(right));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateCast() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                RexNode rexNode = rexNodes.get(0);
                SqlTypeName inputSqlTypeName = rexNode.getType().getSqlTypeName();
                switch (call.type.getSqlTypeName()) {
                    case CHAR:
                    case VARCHAR:
                        return String.format("String.valueOf(%s)", translateToPrimitive(rexNode));
                    case BIGINT:
                        switch (inputSqlTypeName) {
                            case CHAR:
                            case VARCHAR:
                                return String.format("Long.parseLong(%s)", translateToPrimitive(rexNode));
                            default:
                                return String.format("(long) (%s)", translateToPrimitive(rexNode));
                        }
                    case INTEGER:
                        switch (inputSqlTypeName) {
                            case CHAR:
                            case VARCHAR:
                                return String.format("Integer.parseInt(%s)", translateToPrimitive(rexNode));
                            default:
                                return String.format("(int) (%s)", translateToPrimitive(rexNode));
                        }
                    case FLOAT:
                        switch (inputSqlTypeName) {
                            case CHAR:
                            case VARCHAR:
                                return String.format("Float.parseFloat(%s)", translateToPrimitive(rexNode));
                            default:
                                return String.format("(float) (%s)", translateToPrimitive(rexNode));
                        }
                    case DOUBLE:
                        switch (inputSqlTypeName) {
                            case CHAR:
                            case VARCHAR:
                                return String.format("Double.parseDouble(%s)", translateToPrimitive(rexNode));
                            default:
                                return String.format("(double) (%s)", translateToPrimitive(rexNode));
                        }
                    default:
                        throw new IllegalArgumentException(
                                "unexpected sql type in CAST: " + call.type.getSqlTypeName());
                }
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateConcat() {
            return call -> String.join(
                    "+",
                    call.getOperands().stream()
                            .map(tmpCall -> String.format("(%s)", translateToReference(tmpCall)))
                            .collect(Collectors.toList()));
        }

        private MLSqlRexCallImplTable.CallImplementor translateSubstringIndex() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3);
                RexNode first = rexNodes.get(0);
                RexNode second = rexNodes.get(1);
                RexNode third = rexNodes.get(2);
                return String.format(">>basic<<.substringIndex(%s, %s , %s)", translateToReference(first),
                        translateToReference(second), translateToPrimitive(third));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateUnixtimeDiff() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3);
                RexNode first = rexNodes.get(0);
                RexNode second = rexNodes.get(1);
                RexNode third = rexNodes.get(2);
                return String.format(">>basic<<.unixtimeDiff((long) %s, (long) %s, %s)", translateToPrimitive(first),
                        translateToPrimitive(second), translateToReference(third));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translatePower() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                RexNode left = rexNodes.get(0);
                RexNode right = rexNodes.get(1);
                return String.format("Math.pow((double) %s, (double) %s)", translateToPrimitive(left),
                        translateToPrimitive(right));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateLtrim() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                return String.format(">>basic<<.leftTrim(%s)", translateToReference(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateRtrim() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                return String.format(">>basic<<.rightTrim(%s)", translateToReference(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateAbs() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                return String.format("Math.abs(%s)", translateToPrimitive(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateLocate() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2, 3);
                int count = call.getOperands().size();
                if (count == 2) {
                    return String.format("(%s).indexOf(%s) + 1", translateToReference(rexNodes.get(1)),
                            translateToReference(rexNodes.get(0)));
                }
                return String.format("(%s).indexOf(%s, %s - 1) + 1",
                        translateToReference(rexNodes.get(1)),
                        translateToReference(rexNodes.get(0)),
                        translateToPrimitive(rexNodes.get(2)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateSqrt() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                return String.format("Math.sqrt((double) %s)",
                        translateToPrimitive(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateMid() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3);
                return String.format("(%1$s).substring(%2$s, %2$s + %3$s)",
                        translateToReference(rexNodes.get(0)),
                        translateToPrimitive(rexNodes.get(1)),
                        translateToPrimitive(rexNodes.get(2)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateFloor() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                String format;
                if (rexNodes.get(0).getType().getSqlTypeName() == SqlTypeName.FLOAT) {
                    format = "(float) Math.floor(%s)";
                } else {
                    format = "Math.floor(%s)";
                }
                return String.format(format,
                        translateToPrimitive(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateCeil() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                String format;
                if (rexNodes.get(0).getType().getSqlTypeName() == SqlTypeName.FLOAT) {
                    format = "(float) Math.ceil(%s)";
                } else {
                    format = "Math.ceil(%s)";
                }
                return String.format(format,
                        translateToPrimitive(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateTruncate() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                String format;
                if (rexNodes.get(0).getType().getSqlTypeName() == SqlTypeName.FLOAT) {
                    format = "(int) (%1$s * Math.pow(10.0, (double) %2$s)) / (float) Math.pow(10.0, (double) %2$s)";
                } else {
                    format = "(int) (%1$s * Math.pow(10.0, (double) %2$s)) / Math.pow(10.0, (double) %2$s)";
                }
                return String.format(format,
                        translateToPrimitive(rexNodes.get(0)),
                        translateToPrimitive(rexNodes.get(1)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateIf() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3);
                return String.format("(%s) ? (%s) : (%s)",
                        translateToPrimitive(rexNodes.get(0)),
                        translateToPrimitive(rexNodes.get(1)),
                        translateToPrimitive(rexNodes.get(2)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateSplitIndex() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3);
                return String.format(">>basic<<.splitIndex(%s, %s, (int) %s)",
                        translateToReference(rexNodes.get(0)),
                        translateToReference(rexNodes.get(1)),
                        translateToPrimitive(rexNodes.get(2)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateregexpExtract() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3);
                return String.format(">>basic<<.regexpExtract(%s, %s, (int) %s)",
                        translateToReference(rexNodes.get(0)),
                        translateToReference(rexNodes.get(1)),
                        translateToPrimitive(rexNodes.get(2)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateSubstring() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2, 3);
                if (rexNodes.size() == 2) {
                    return String.format(">>basic<<.substringByRt(%s, %s)",
                            translateToReference(rexNodes.get(0)),
                            translateToPrimitive(rexNodes.get(1)));
                }
                return String.format(">>basic<<.substringByRt(%s, %s, (int) %s)",
                        translateToReference(rexNodes.get(0)),
                        translateToPrimitive(rexNodes.get(1)),
                        translateToPrimitive(rexNodes.get(2)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateUpper() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                return String.format("(%s).toUpperCase()",
                        translateToReference(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateLower() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                return String.format("(%s).toLowerCase()",
                        translateToReference(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateInetAton() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1, 2);
                if (rexNodes.size() == 1) {
                    return String.format(">>basic<<.INetAToN(%s)", translateToReference(rexNodes.get(0)));
                }
                return String.format(">>basic<<.INetAToN(%s, %s)",
                        translateToReference(rexNodes.get(0)),
                        translateToReference(rexNodes.get(1)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateTrim() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3);
                return String.format("(%s).trim()", translateToReference(rexNodes.get(2)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateConcatWs() {
            return call -> {
                List<RexNode> rexNodes = call.getOperands();
                RexNode separator = rexNodes.get(0);
                List<RexNode> copyNodes = new ArrayList<>(rexNodes);
                copyNodes.remove(separator);
                return String.format("(%s)", String.join(
                        String.format(" + %s + ", translateToReference(separator)),
                        copyNodes.stream()
                                .map(copyNode -> String.format("(%s)", translateToReference(copyNode)))
                                .collect(Collectors.toList())
                ));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateMod() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                return String.format("(int) (%s %% %s)", translateToPrimitive(rexNodes.get(0)),
                        translateToPrimitive(rexNodes.get(1)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateNow() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 0, 1);
                if (rexNodes.size() == 0) {
                    return ">>basic<<.currentDatetime()";
                }
                return String.format(">>basic<<.currentDatetime(%s)", translateToReference(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateInetNtoa() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1, 2);
                if (rexNodes.size() == 1) {
                    return String.format(">>basic<<.INetNToA((long) %s)", translateToPrimitive(rexNodes.get(0)));
                }
                return String.format(">>basic<<.INetNToA((long) %s, %s)",
                        translateToPrimitive(rexNodes.get(0)),
                        translateToReference(rexNodes.get(1)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateContainsSubstring() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                return String.format("(%s).contains(%s)", translateToReference(rexNodes.get(0)),
                        translateToReference(rexNodes.get(1)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateUnixTimestamp() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 0, 2);
                if (rexNodes.size() == 0) {
                    return "(int)(System.currentTimeMillis()/1000)";
                }
                if (rexNodes.size() == 1) {
                    return String.format(">>basic<<.unixTimestamp(%s)", translateToReference(rexNodes.get(0)));
                }
                return String.format(">>basic<<.unixTimestamp(%s, %s)", translateToReference(rexNodes.get(0)),
                        translateToReference(rexNodes.get(1)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateCurDate() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 0, 1);
                if (rexNodes.size() == 0) {
                    return ">>basic<<.curDate()";
                }
                return String.format(">>basic<<.curDate(%s)", translateToReference(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateFromUnixTime() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1, 2);
                if (rexNodes.size() == 1) {
                    return String.format(">>basic<<.fromUnixTime((long) %s)", translateToPrimitive(rexNodes.get(0)));
                }
                return String.format(">>basic<<.fromUnixTime((long) %s, %s)", translateToPrimitive(rexNodes.get(0)),
                        translateToReference(rexNodes.get(1)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateReplace() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3);
                return String.format("(%s).replace(%s, %s)",
                        translateToReference(rexNodes.get(0)),
                        translateToReference(rexNodes.get(1)),
                        translateToReference(rexNodes.get(2)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateInstr() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                return String.format("(%s).indexOf(%s) + 1",
                        translateToReference(rexNodes.get(0)),
                        translateToReference(rexNodes.get(1)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateIntOverflow() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 1);
                return String.format(">>basic<<.intOverflow(%s)", translateToPrimitive(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateRegexp() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 2);
                return String.format("java.util.regex.Pattern.compile(%s).matcher(%s).find()",
                        translateToReference(rexNodes.get(1)),
                        translateToReference(rexNodes.get(0)));
            };
        }

        private MLSqlRexCallImplTable.CallImplementor translateCase() {
            return call -> {
                List<RexNode> rexNodes = assertOpCount(call, 3, Integer.MAX_VALUE);
                return implementCase0(rexNodes, 0);
            };
        }

        private String implementCase0(List<RexNode> rexNodes, int i) {
            if (i == rexNodes.size() - 1) {
                return translateToPrimitive(rexNodes.get(i));
            }
            return String.format("(%s) ? (%s) : (%s)",
                    translateToPrimitive(rexNodes.get(i)),
                    translateToPrimitive(rexNodes.get(i + 1)),
                    implementCase0(rexNodes, i + 2));
        }

        private List<RexNode> assertOpCount(RexCall call, int count) {
            List<RexNode> rexNodes = call.getOperands();
            if (rexNodes.size() != count) {
                throw new IllegalArgumentException("illegal operator count: " + rexNodes.size());
            }
            return rexNodes;
        }

        private List<RexNode> assertOpCount(RexCall call, int minCount, int maxCount) {
            List<RexNode> rexNodes = call.getOperands();
            if (rexNodes.size() < minCount || rexNodes.size() > maxCount) {
                throw new IllegalArgumentException("illegal operator count: " + rexNodes.size());
            }
            return rexNodes;
        }

        private void checkTypeConsistent(RexNode left, RexNode right, String operator) {
            String showOperator;
            switch (operator) {
                case "==":
                    showOperator = "=";
                    break;
                case "!=":
                    showOperator = "<>";
                    break;
                default:
                    showOperator = operator;
            }

            if (CHARACTER_TYPES.contains(left.getType().getSqlTypeName())
                    != CHARACTER_TYPES.contains(right.getType().getSqlTypeName())) {
                throw new MessageLocalizedException("comparison.type.error",
                        new Object[]{showOperator}, MLSqlRels.class);
            }
        }

        private String addPrimitiveValueGetter(String var, RelDataType type) {
            final String pattern;
            switch (type.getSqlTypeName()) {
                case INTEGER:
                    pattern = "%s.intValue()";
                    break;
                case BIGINT:
                    pattern = "%s.longValue()";
                    break;
                case FLOAT:
                    pattern = "%s.floatValue()";
                    break;
                case DOUBLE:
                case DECIMAL:
                    pattern = "%s.doubleValue()";
                    break;
                default:
                    pattern = "%s";
            }
            return String.format(pattern, var);
        }

        private String addReferenceValueGetter(String var, RelDataType type) {
            final String pattern;
            switch (type.getSqlTypeName()) {
                case INTEGER:
                    pattern = ">>basic<<.toInteger(%s)";
                    break;
                case BIGINT:
                    pattern = ">>basic<<.toLong(%s)";
                    break;
                case FLOAT:
                    pattern = ">>basic<<.toFloat(%s)";
                    break;
                case DOUBLE:
                case DECIMAL:
                    pattern = ">>basic<<.toDouble(%s)";
                    break;
                default:
                    pattern = "%s";
            }
            return String.format(pattern, var);
        }

        private String translateToPrimitive(RexNode rexNode) {
            return addPrimitiveValueGetter(translateToReference(rexNode), rexNode.getType());
        }

        private String translateToReference(RexNode rexNode) {
            return addReferenceValueGetter(translate(rexNode), rexNode.getType());
        }

        public String translate(RexNode rexNode) {
            return rexNode.accept(this);
        }
    }

    public static class MLSqlOriginsFinder extends SimpleRexVisitorImpl<Set<String>> {

        private final List<String> fieldNames;

        protected MLSqlOriginsFinder(List<String> fieldNames) {
            super();
            this.fieldNames = fieldNames;
        }

        @Override
        public Set<String> visitInputRef(RexInputRef inputRef) {
            return ImmutableSet.of(fieldNames.get(inputRef.getIndex()));
        }

        @Override
        public Set<String> visitLiteral(RexLiteral literal) {
            return ImmutableSet.of();
        }

        @Override
        public Set<String> visitCall(RexCall call) {
            List<RexNode> operands = call.getOperands();
            return operands.stream()
                    .flatMap(rexNode -> find(rexNode).stream())
                    .collect(Collectors.toSet());
        }

        public Set<String> find(RexNode rexNode) {
            return rexNode.accept(this);
        }
    }
}
