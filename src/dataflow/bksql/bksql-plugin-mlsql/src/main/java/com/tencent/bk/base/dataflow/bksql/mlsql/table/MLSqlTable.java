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

package com.tencent.bk.base.dataflow.bksql.mlsql.table;

import com.google.common.collect.ImmutableSet;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlRel;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlTableScan;
import com.tencent.bk.base.datalab.bksql.table.ColumnMetadata;
import com.tencent.bk.base.datalab.bksql.table.TableMetadata;
import com.tencent.bk.base.datalab.bksql.util.DataTypeMapper;
import java.util.AbstractMap;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

public class MLSqlTable extends AbstractTable implements TranslatableTable {

    private static final Set<String> RESERVED_COLUMNS = ImmutableSet.of("timestamp", "offset");

    private final TableMetadata<ColumnMetadata> tableMeta;
    private final DataTypeMapper<RelDataType> typeMapper;
    private final String tableName;

    public MLSqlTable(String tableName, TableMetadata<ColumnMetadata> tableMeta,
            DataTypeMapper<RelDataType> typeMapper) {
        this.tableMeta = tableMeta;
        this.typeMapper = typeMapper;
        this.tableName = tableName;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        return relDataTypeFactory.builder()
                .addAll(tableMeta.listColumns()
                        .stream()
                        .filter(columnMetadata -> !RESERVED_COLUMNS.contains(columnMetadata.getColumnName()))
                        .map(columnMetadata ->
                                new AbstractMap.SimpleEntry<>(
                                        columnMetadata.getColumnName(),
                                        typeMapper.toExternalType(columnMetadata.getDataType())))
                        .collect(Collectors.toList()))
                .build();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        RelOptCluster cluster = toRelContext.getCluster();
        return new MLSqlTableScan(tableName, cluster, cluster.traitSetOf(MLSqlRel.CONVENTION), relOptTable);
    }

    public String getTableName() {
        return tableName;
    }
}
