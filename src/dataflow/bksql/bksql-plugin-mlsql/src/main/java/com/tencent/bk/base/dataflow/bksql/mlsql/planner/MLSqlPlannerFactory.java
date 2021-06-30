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

package com.tencent.bk.base.dataflow.bksql.mlsql.planner;

import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.MLSqlFunctionConstants;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.MLSqlOperatorTable;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelLateralFunciton;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlTypeFactory;
import com.tencent.bk.base.dataflow.bksql.mlsql.rule.MLSqlRuleSets;
import com.tencent.bk.base.dataflow.bksql.mlsql.parser.MLSqlSqlParserImpl;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.json.JSONObject;

public class MLSqlPlannerFactory {

    public static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder()
            .setCaseSensitive(true)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setQuotedCasing(Casing.UNCHANGED)
            .setQuoting(Quoting.BACK_TICK)
            .setConformance(SqlConformanceEnum.DEFAULT)
            .setParserFactory(new MLSqlSqlParserImpl.MLSqlSqlParserFactory())
            .build();
    public static final SqlToRelConverter.Config CONVERTER_CONFIG = SqlToRelConverter.configBuilder()
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withTrimUnusedFields(true)
            .build();
    private static final MLSqlPlannerFactory INSTANCE = new MLSqlPlannerFactory();
    private static MLSqlTypeFactory typeFactory = new MLSqlTypeFactory(RelDataTypeSystem.DEFAULT);

    private MLSqlPlannerFactory() {

    }

    public static MLSqlPlannerFactory get() {
        return INSTANCE;
    }

    public Planner withDefaultSchema(Schema defaultSchema) {
        final FrameworkConfig frameworkConfig = Frameworks
                .newConfigBuilder()
                .parserConfig(PARSER_CONFIG)
                .traitDefs(ConventionTraitDef.INSTANCE)
                .convertletTable(StandardConvertletTable.INSTANCE)
                .operatorTable(MLSqlOperatorTable.instance())
                .ruleSets(ImmutableList.of(MLSqlRuleSets.instance()))
                .context(Contexts.empty())
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .sqlToRelConverterConfig(CONVERTER_CONFIG)
                .defaultSchema(CalciteSchema.createRootSchema(true, false).plus()
                        .add("default", defaultSchema))
                .build();
        return new MLSqlPlannerImpl(frameworkConfig);
    }

    /**
     * 注册Lateral函数
     *
     * @param defaultSchema schema对象
     * @param lateralFunction lateral函数基本信息
     * @return 返回执行计算Planner
     */
    public Planner withDefaultSchema(Schema defaultSchema, JSONObject lateralFunction) {
        MLSqlModelLateralFunciton function = null;
        if (lateralFunction != null) {
            String functionName = lateralFunction.getString(MLSqlFunctionConstants.LATERAL_FUNCTION_NAME);
            int lateralReturnSize = lateralFunction.getInt(MLSqlFunctionConstants.LATERAL_RETURN_SIZE);
            List<String> nameList = new ArrayList<>();
            List<RelDataType> typeList = new ArrayList<>();
            for (int index = 0; index < lateralReturnSize; index++) {
                nameList.add("column_" + index);
                typeList.add(typeFactory.createSqlType(SqlTypeName.ANY));
            }
            function = new MLSqlModelLateralFunciton(functionName, typeList, nameList);
        }
        final FrameworkConfig frameworkConfig = Frameworks
                .newConfigBuilder()
                .parserConfig(PARSER_CONFIG)
                .traitDefs(ConventionTraitDef.INSTANCE)
                .convertletTable(StandardConvertletTable.INSTANCE)
                .operatorTable(MLSqlOperatorTable.instance(function))
                .ruleSets(ImmutableList.of(MLSqlRuleSets.instance()))
                .context(Contexts.empty())
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .sqlToRelConverterConfig(CONVERTER_CONFIG)
                .defaultSchema(CalciteSchema.createRootSchema(true, false).plus()
                        .add("default", defaultSchema))
                .build();
        return new MLSqlPlannerImpl(frameworkConfig);
    }
}
