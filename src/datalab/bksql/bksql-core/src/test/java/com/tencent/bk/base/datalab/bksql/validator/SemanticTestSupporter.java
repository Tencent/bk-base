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

package com.tencent.bk.base.datalab.bksql.validator;

import java.util.Map;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

public class SemanticTestSupporter {

    private static final SqlTypeFactoryImpl TYPE_FACTORY = new SqlTypeFactoryImpl(
            RelDataTypeSystem.DEFAULT);
    private static final RelDataTypeSystem TYPE_SYSTEM = RelDataTypeSystem.DEFAULT;
    private static CalciteCatalogReader calciteCatalogReader;
    private static SchemaPlus schemaPlus;

    protected static void initReader(Map<String, RelProtoDataType> udtMap,
            Map<String, Function> udfMap) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        schemaPlus = rootSchema.plus();
        //添加表Test
        rootSchema.add("tab", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = new RelDataTypeFactory
                        .Builder(TYPE_FACTORY);
                //列id, 类型int
                builder.add("thedate", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER));
                builder.add("dtEventTimeStamp", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.BIGINT));
                builder.add("ip", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER));
                builder.add("activeWorkers", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.INTEGER));
                //列name, 类型为varchar
                builder.add("name", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
                builder.add("localtime", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
                builder.add("time_str", new BasicSqlType(TYPE_SYSTEM, SqlTypeName.VARCHAR));
                return builder.build();
            }
        });
        addFunction(udfMap);
        addType(udtMap);
        calciteCatalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema(schemaPlus)),
                CalciteSchema.from(rootSchema(schemaPlus))
                        .path(null),
                new SqlTypeFactoryImpl(
                        RelDataTypeSystem.DEFAULT),
                null);
    }

    protected static void addFunction(Map<String, Function> udfMap) {
        if (udfMap != null) {
            udfMap.forEach((key, value) -> schemaPlus.add(key, value));
        }
    }

    protected static void addType(Map<String, RelProtoDataType> udtMap) {
        if (udtMap != null) {
            udtMap.forEach((key, value) -> schemaPlus.add(key, value));
        }
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (; ; ) {
            if (schema.getParentSchema() == null) {
                return schema;
            }
            schema = schema.getParentSchema();
        }
    }

    protected static SqlNode parseQuery(String sql) throws SqlParseException {
        final SqlParser.Config config =
                SqlParser.configBuilder()
                        .setParserFactory(SqlParserImpl.FACTORY)
                        .setCaseSensitive(false)
                        .setQuoting(Quoting.BACK_TICK)
                        .setQuotedCasing(Casing.UNCHANGED)
                        .setUnquotedCasing(Casing.UNCHANGED)
                        .setConformance(SqlConformanceEnum.LENIENT)
                        .build();
        SqlParser parsed = SqlParser.create(sql, config);
        return parsed.parseQuery();
    }

    protected static boolean check(String sql) {
        boolean checkResult = true;
        SqlValidatorImpl validatorImpl = getSqlValidator(calciteCatalogReader);
        SqlNode originNode;
        try {
            originNode = parseQuery(sql);
            SqlNode validatedNode = validatorImpl.validate(originNode);
            if (validatedNode != null) {
                checkResult = true;
            }
        } catch (SqlParseException e) {
            checkResult = false;
        } catch (Exception e) {
            checkResult = false;
        }
        return checkResult;
    }

    private static SqlValidatorImpl getSqlValidator(CalciteCatalogReader catalogReader) {
        SqlOperatorTable sqlOperatorTable = ChainedSqlOperatorTable
                .of(SqlStdOperatorTable.instance(), catalogReader);
        SqlValidatorImpl validatorImpl = (SqlValidatorImpl) SqlValidatorUtil
                .newValidator(sqlOperatorTable, catalogReader,
                        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
                        SqlConformanceEnum.LENIENT);
        validatorImpl.setSkipFunctionValid(false);
        return validatorImpl;
    }
}
