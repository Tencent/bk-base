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

import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlDeParser;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.ErrorCode;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.MLSqlLocalizedException;
import com.tencent.bk.base.dataflow.bksql.mlsql.schema.MLSqlSchema;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTask.AbstractField;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskContext;
import com.tencent.bk.base.datalab.bksql.table.BlueKingTrtTableMetadataConnector;
import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang.StringUtils;

public class MLSqlTableRegistry {

    private final MLSqlTableFactoryImpl standardImpl;
    private final MLSqlSchema schema;

    public MLSqlTableRegistry(
            String metadataUrl,
            MLSqlSchema schema,
            RelDataTypeFactory typeFactory,
            MLSqlTaskContext context) {
        if (StringUtils.isBlank(metadataUrl)) {
            throw new MLSqlLocalizedException("miss.config.error",
                    new Object[]{"trtTableMetadataUrlPattern"},
                    MLSqlDeParser.class,
                    ErrorCode.MISS_CONFIG_ERROR);
        }
        this.standardImpl = new MLSqlTableFactoryImpl(
                BlueKingTrtTableMetadataConnector.forUrl(metadataUrl),
                typeFactory,
                context);
        this.schema = schema;
    }

    public void register(String name) {
        Table table;
        table = standardImpl.create(name);
        schema.registerTable(name, table);
    }

    public void registerVirtualTable(String name, List<AbstractField> fieldList) {
        Table table = standardImpl.createVirtualTable(name, fieldList);
        schema.registerTable(name, table);
    }

    public Table getTable(String name) {
        return schema.getTable(name);
    }

}
