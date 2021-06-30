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
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlTypeFactory;
import com.tencent.bk.base.dataflow.bksql.mlsql.sql2rel.MLSqlSqlToRelConverter;
import java.io.Reader;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

public class MLSqlPlannerImpl implements Planner, RelOptTable.ViewExpander {

    private final FrameworkConfig config;
    private final SchemaPlus defaultSchema;
    private final SqlParser.Config parserConfig;
    private final SqlOperatorTable operatorTable;
    private final JavaTypeFactoryImpl typeFactory;
    private final RelOptPlanner planner;
    private final SqlToRelConverter.Config sqlToRelConverterConfig;
    private final SqlRexConvertletTable convertletTable;
    private final List<Program> programs;
    private SqlValidatorImpl validator;
    private RelRoot root;

    public MLSqlPlannerImpl(FrameworkConfig config) {
        this.config = config;
        parserConfig = config.getParserConfig();
        defaultSchema = config.getDefaultSchema();
        operatorTable = config.getOperatorTable();
        sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
        convertletTable = config.getConvertletTable();
        programs = config.getPrograms();
        typeFactory = new MLSqlTypeFactory(config.getTypeSystem());
        planner = createPlanner(config);
    }

    private static SchemaPlus rootSchema(final SchemaPlus schema) {
        SchemaPlus tempSchema = schema;
        for (; ; ) {
            if (tempSchema.getParentSchema() == null) {
                return tempSchema;
            }
            tempSchema = tempSchema.getParentSchema();
        }
    }

    private RelOptPlanner createPlanner(FrameworkConfig config) {
        VolcanoPlanner planner = new VolcanoPlanner(config.getCostFactory(), Contexts.empty());
        config.getTraitDefs().forEach(planner::addRelTraitDef);
        return planner;
    }

    @Override
    public SqlNode parse(String s) throws SqlParseException {
        SqlParser parser = SqlParser.create(s, config.getParserConfig());
        return parser.parseStmt();
    }

    @Override
    public SqlNode parse(Reader source) throws SqlParseException {
        return null;
    }

    @Override
    public SqlNode validate(SqlNode sqlNode) throws ValidationException {
        final SqlConformance conformance = conformance();
        final CalciteCatalogReader catalogReader = createCatalogReader();
        validator = new MLSqlSqlValidator(
                operatorTable,
                catalogReader,
                typeFactory,
                conformance
        );
        validator.setIdentifierExpansion(true);
        return validator.validate(sqlNode);
    }

    @Override
    public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
        SqlNode validated = validate(sqlNode);
        final RelDataType type =
                this.validator.getValidatedNodeType(validated);
        return Pair.of(validated, type);
    }

    @Override
    public RelRoot rel(SqlNode sqlNode) throws RelConversionException {
        final RexBuilder rexBuilder = createRexBuilder();
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        final SqlToRelConverter sqlToRelConverter =
                new MLSqlSqlToRelConverter(this, validator,
                        createCatalogReader(), cluster, convertletTable, sqlToRelConverterConfig);
        root =
                sqlToRelConverter.convertQuery(sqlNode, false, true);
        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        root = root.withRel(
                RelDecorrelator.decorrelateQuery(root.rel));
        return root;
    }

    @Override
    public RelNode convert(SqlNode sqlNode) throws RelConversionException {
        return rel(sqlNode).rel;
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }

    @Override
    public RelNode transform(int ruleSetIndex, RelTraitSet relTraitSet, RelNode relNode) throws RelConversionException {
        relNode.getCluster().setMetadataProvider(
                new CachingRelMetadataProvider(
                        relNode.getCluster().getMetadataProvider(),
                        relNode.getCluster().getPlanner()));
        Program program = programs.get(ruleSetIndex);
        return program.run(planner, relNode, relTraitSet, ImmutableList.of(),
                ImmutableList.of());
    }

    @Override
    public void reset() {

    }

    @Override
    public void close() {

    }

    @Override
    public RelTraitSet getEmptyTraitSet() {
        return planner.emptyTraitSet();
    }

    @Override
    public RelRoot expandView(RelDataType relDataType, String s, List<String> schemaPath, List<String> viewPath) {

        SqlParser parser = SqlParser.create(s, parserConfig);
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException("parse failed", e);
        }

        final SqlConformance conformance = conformance();
        final CalciteCatalogReader catalogReader =
                createCatalogReader().withSchemaPath(schemaPath);
        final SqlValidator validator =
                new MLSqlSqlValidator(operatorTable, catalogReader, typeFactory,
                        conformance);
        validator.setIdentifierExpansion(true);

        final RexBuilder rexBuilder = createRexBuilder();
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        final SqlToRelConverter.Config config = SqlToRelConverter
                .configBuilder()
                .withConfig(sqlToRelConverterConfig)
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .build();
        final SqlToRelConverter sqlToRelConverter =
                new MLSqlSqlToRelConverter(this, validator,
                        catalogReader, cluster, convertletTable, config);

        final RelRoot root =
                sqlToRelConverter.convertQuery(sqlNode, true, false);
        final RelRoot root2 =
                root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        return root2.withRel(
                RelDecorrelator.decorrelateQuery(root.rel));
    }
    // CalciteCatalogReader is stateless; no need to store one

    private SqlConformance conformance() {
        final Context context = config.getContext();
        if (context != null) {
            final CalciteConnectionConfig connectionConfig =
                    context.unwrap(CalciteConnectionConfig.class);
            if (connectionConfig != null) {
                return connectionConfig.conformance();
            }
        }
        return parserConfig.conformance();
    }

    private CalciteCatalogReader createCatalogReader() {
        final SchemaPlus rootSchema = rootSchema(defaultSchema);
        final Context context = config.getContext();
        final CalciteConnectionConfig connectionConfig;

        if (context != null) {
            connectionConfig = context.unwrap(CalciteConnectionConfig.class);
        } else {
            Properties properties = new Properties();
            properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                    String.valueOf(parserConfig.caseSensitive()));
            connectionConfig = new CalciteConnectionConfigImpl(properties);
        }
        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(defaultSchema).path(null),
                typeFactory,
                connectionConfig);
    }
    // RexBuilder is stateless; no need to store one

    private RexBuilder createRexBuilder() {
        return new RexBuilder(typeFactory);
    }
}
