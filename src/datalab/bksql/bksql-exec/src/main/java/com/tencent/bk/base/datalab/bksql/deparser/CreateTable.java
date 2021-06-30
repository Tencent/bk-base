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

package com.tencent.bk.base.datalab.bksql.deparser;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public class CreateTable extends SimpleListenerDeParser {

    private static final String CREATE_TABLE_NAME = "create_table_name";
    private static final String COLUMN_SCHEMA = "column_schema";
    private Map<String, Object> deParseMap = Maps.newHashMap();

    @Override
    public void enterCreateTableNode(SqlCreateTable createTable) {
        super.enterCreateTableNode(createTable);
        Preconditions.checkArgument(createTable != null);
        SqlIdentifier tableName = createTable.operand(0);
        SqlNodeList columnSchema = createTable.operand(1);
        SqlNode query = createTable.operand(2);
        //这里判断是否是简单的建表语句，而不是CTAS
        boolean isCreateTable = tableName != null
                && columnSchema != null && query == null;
        if (isCreateTable) {
            deParseMap.putIfAbsent(CREATE_TABLE_NAME, tableName.toString());
            deParseMap.putIfAbsent(COLUMN_SCHEMA, getTableSchema(columnSchema));
        }
    }

    /**
     * 获取表结构Map
     *
     * @param columnSchema 列定义
     * @return 返回表结构map
     */
    private Map<String, String> getTableSchema(SqlNodeList columnSchema) {
        LinkedHashMap<String, String> tableSchema = Maps.newLinkedHashMap();
        for (SqlNode node : columnSchema) {
            SqlColumnDeclaration columnDef = (SqlColumnDeclaration) node;
            SqlIdentifier columnName = columnDef.operand(0);
            SqlDataTypeSpec columnType = columnDef.operand(1);
            SqlIdentifier typeName = columnType.getTypeNameSpec().getTypeName();
            tableSchema.putIfAbsent(columnName.toString(), typeName.toString());
        }
        return tableSchema;
    }

    @Override
    protected Object getRetObj() {
        return deParseMap;
    }
}
