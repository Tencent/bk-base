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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class SparkSqlTableEntity {

    private final String id;
    private final String name;
    private final String sql;
    private final List<Field> fields;

    private SparkSqlTableEntity(String id, String name, String sql, List<Field> fields) {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(sql);
        Preconditions.checkNotNull(fields);
        this.id = id;
        this.name = name;
        this.sql = sql;
        this.fields = fields;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getSql() {
        return sql;
    }

    public List<Field> getFields() {
        return fields;
    }

    public static class Field {

        private final String type;
        private String alias;
        private final String field;
        private final boolean dimension;
        private final int index;

        public Field(int index, String field, String alias, String type, boolean dimension) {
            this.index = index;
            this.field = field;
            this.alias = alias;
            this.type = type;
            this.dimension = dimension;
        }

        @JsonProperty("field_type")
        public String getType() {
            return type;
        }

        @JsonProperty("field_alias")
        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        @JsonProperty("field_name")
        public String getField() {
            return field;
        }

        @JsonProperty("is_dimension")
        public boolean getDimension() {
            return dimension;
        }

        @JsonProperty("field_index")
        public int getIndex() {
            return index;
        }
    }

    public static class Builder {

        private String id;
        private String name;
        private String sql;
        private List<Field> fields = new ArrayList<>();

        private Builder() {
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder sql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder addField(Field field) {
            fields.add(field);
            return this;
        }

        public SparkSqlTableEntity create() {
            return new SparkSqlTableEntity(id, name, sql, fields);
        }
    }
}
