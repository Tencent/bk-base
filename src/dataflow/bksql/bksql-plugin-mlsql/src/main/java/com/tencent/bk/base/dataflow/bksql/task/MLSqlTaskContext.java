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

package com.tencent.bk.base.dataflow.bksql.task;

import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlRel;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.RelNode;

public class MLSqlTaskContext {

    private final String sql;
    private final String tableName;
    private final RelNode relRoot;
    private final AtomicInteger nextID = new AtomicInteger(0);

    public MLSqlTaskContext(
            String sql,
            String tableName,
            RelNode relRoot) {
        this.sql = sql;
        this.tableName = tableName;
        this.relRoot = relRoot;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(MLSqlTaskContext context) {
        return new Builder(context);
    }

    public String getSql() {
        return sql;
    }

    public RelNode getRelRoot() {
        return relRoot;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isRoot(RelNode relNode) {
        return relNode == relRoot;
    }

    public int getNextID() {
        return nextID.incrementAndGet();
    }

    public MLSqlTaskContext withRoot(MLSqlRel root) {
        return builder(this).setRelRoot(root).create();
    }

    public static class Builder {

        private String sql = null;
        private String tableName = null;
        private RelNode relRoot = null;

        private Builder() {

        }

        private Builder(MLSqlTaskContext context) {
            sql = context.getSql();
            relRoot = context.getRelRoot();
            tableName = context.getTableName();
        }

        public Builder setSql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setRelRoot(RelNode relRoot) {
            this.relRoot = relRoot;
            return this;
        }

        public MLSqlTaskContext create() {
            return new MLSqlTaskContext(sql, tableName, relRoot);
        }
    }
}
