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

import com.tencent.blueking.bksql.table.ColumnMetadata;
import com.tencent.blueking.bksql.table.ColumnMetadataImpl;
import com.tencent.blueking.bksql.util.BlueKingDataTypeMapper;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelfDependencyParser {

    public static String SELFDEPENDENCY_TABLE_NAME_KEY = "table_name";
    public static String SELFDEPENDENCY_TABLE_FIELDS_KEY = "table_fields";

    public static String SELFDEPENDENCY_TABLE_FIELD_NAME_KEY = "field_name";
    public static String SELFDEPENDENCY_TABLE_FIELD_TYPE_KEY = "field_type";
    public static String SELFDEPENDENCY_TABLE_FIELD_DESCRIPTION_KEY = "description";

    private Config selfDependencyConfig;

    private String tableName;
    private List<ColumnMetadata> metaDatas = new ArrayList<>();
    private List<SelfDependencyField> fields = new ArrayList<>();

    private Map<String, String> nameToAliasMap = new HashMap();

    private boolean isReady = false;

    public SelfDependencyParser(Config selfDependencyConfig) {
        if (selfDependencyConfig != null) {
            this.selfDependencyConfig = selfDependencyConfig;
            isReady = parseSelfDependTableConfig();
        }
    }

    public List<ColumnMetadata> getSelfDependTableMetaList() {
        return metaDatas;
    }

    public String getSelfDependencyTableName() {
        return tableName;
    }

    public boolean isReady() {
        return isReady;
    }

    private boolean parseSelfDependTableConfig() {
        if (!checkSelfDependTableConfig()) {
            return false;
        }
        this.tableName = this.selfDependencyConfig.getString(SELFDEPENDENCY_TABLE_NAME_KEY);
        List<? extends Config> tableFieldsConfig =
                this.selfDependencyConfig.getConfigList(SELFDEPENDENCY_TABLE_FIELDS_KEY);
        BlueKingDataTypeMapper dataTypeMapper = new BlueKingDataTypeMapper();

        for (Config fieldConfig : tableFieldsConfig) {
            ColumnMetadata columnMetadata =
                    new ColumnMetadataImpl(
                            fieldConfig.getString(SELFDEPENDENCY_TABLE_FIELD_NAME_KEY),
                            dataTypeMapper
                                    .toBKSqlType(
                                            fieldConfig.getString(SELFDEPENDENCY_TABLE_FIELD_TYPE_KEY).toLowerCase()),
                            fieldConfig.getString(SELFDEPENDENCY_TABLE_FIELD_NAME_KEY));
            metaDatas.add(columnMetadata);
            //This is used to check input schema and output schema
            SelfDependencyField field = new SelfDependencyField(
                    fieldConfig.getString(SELFDEPENDENCY_TABLE_FIELD_NAME_KEY),
                    fieldConfig.getString(SELFDEPENDENCY_TABLE_FIELD_TYPE_KEY).toLowerCase(),
                    fieldConfig.getString(SELFDEPENDENCY_TABLE_FIELD_DESCRIPTION_KEY));
            fields.add(field);

            nameToAliasMap.put(
                    fieldConfig.getString(SELFDEPENDENCY_TABLE_FIELD_NAME_KEY),
                    fieldConfig.getString(SELFDEPENDENCY_TABLE_FIELD_DESCRIPTION_KEY));
        }

        if (metaDatas.size() == 0) {
            throw new IllegalArgumentException(
                    "Found empty schema definition for self dependency table, please added schema");
        }
        return true;
    }

    public String getColumnAlias(String name) {
        if (nameToAliasMap.containsKey(name)) {
            return nameToAliasMap.get(name);
        }
        return null;
    }

    private boolean checkSelfDependTableConfig() {
        return this.selfDependencyConfig.hasPath(SELFDEPENDENCY_TABLE_NAME_KEY)
                && this.selfDependencyConfig.hasPath(SELFDEPENDENCY_TABLE_FIELDS_KEY);
    }

    public List<SelfDependencyField> getFields() {
        return fields;
    }

    public static class SelfDependencyField {

        private String fieldName;
        private String fieldType;
        private String fieldAlas;

        public SelfDependencyField(String fieldName, String fieldType, String fieldAlas) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.fieldAlas = fieldAlas;
        }


        public String getFieldName() {
            return fieldName;
        }

        public String getFieldType() {
            return fieldType;
        }

        public String getFieldAlas() {
            return fieldAlas;
        }
    }
}
