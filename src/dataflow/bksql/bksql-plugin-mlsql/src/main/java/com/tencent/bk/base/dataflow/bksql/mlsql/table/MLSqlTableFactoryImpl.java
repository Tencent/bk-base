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

import com.tencent.bk.base.dataflow.bksql.task.MLSqlTask.AbstractField;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskContext;
import com.tencent.bk.base.dataflow.bksql.util.MLSqlDataTypeMapper;
import com.tencent.bk.base.datalab.bksql.table.ColumnMetadata;
import com.tencent.bk.base.datalab.bksql.table.ColumnMetadataImpl;
import com.tencent.bk.base.datalab.bksql.table.MapBasedTableMetadata;
import com.tencent.bk.base.datalab.bksql.table.TableMetadata;
import com.tencent.bk.base.datalab.bksql.table.TableMetadataConnector;
import com.tencent.bk.base.datalab.bksql.util.BlueKingDataTypeMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;

public class MLSqlTableFactoryImpl implements MLSqlTableFactory {

    final TableMetadataConnector<ColumnMetadata> tableMetadataConnector;
    final MLSqlDataTypeMapper typeMapper;
    final MLSqlTaskContext context;

    public MLSqlTableFactoryImpl(TableMetadataConnector<ColumnMetadata> tableMetadataConnector,
            RelDataTypeFactory relDataTypeFactory,
            MLSqlTaskContext context) {
        this.tableMetadataConnector = tableMetadataConnector;
        typeMapper = new MLSqlDataTypeMapper(relDataTypeFactory);
        this.context = context;
    }

    public Table create(String tableName) {
        String physicalName = tableName;
        return new MLSqlTable(
                tableName,
                tableMetadataConnector.fetchTableMetadata(physicalName),
                typeMapper);
    }

    public Table createVirtualTable(String tableName, List<AbstractField> fieldList) {
        Map<String, ColumnMetadata> map = new HashMap();
        for (AbstractField field : fieldList) {
            BlueKingDataTypeMapper mapper = new BlueKingDataTypeMapper();
            ColumnMetadata columnMetadata = new ColumnMetadataImpl(field.getField(),
                    mapper.toBKSqlType(field.getType()), field.getDescription());
            map.put(field.getField(), columnMetadata);
        }
        TableMetadata<ColumnMetadata> metadata = MapBasedTableMetadata.wrap(map);
        return new MLSqlTable(tableName, metadata, typeMapper);

    }
}
