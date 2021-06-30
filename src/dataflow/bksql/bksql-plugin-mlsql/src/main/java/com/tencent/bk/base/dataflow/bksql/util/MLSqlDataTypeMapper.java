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

package com.tencent.bk.base.dataflow.bksql.util;

import com.tencent.bk.base.datalab.bksql.util.DataType;
import com.tencent.bk.base.datalab.bksql.util.DataTypeMapper;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public class MLSqlDataTypeMapper implements DataTypeMapper<RelDataType> {

    private final RelDataTypeFactory relDataTypeFactory;

    public MLSqlDataTypeMapper(RelDataTypeFactory relDataTypeFactory) {
        this.relDataTypeFactory = relDataTypeFactory;
    }

    @Override
    public DataType toBKSqlType(RelDataType fromType) {
        switch (fromType.getSqlTypeName()) {
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return DataType.INTEGER;
            case BIGINT:
                return DataType.LONG;
            case FLOAT:
                return DataType.FLOAT;
            case DOUBLE:
            case DECIMAL:
                return DataType.DOUBLE;
            case CHAR:
            case VARCHAR:
                return DataType.STRING;
            case BOOLEAN:
                return DataType.BOOLEAN;
            case TIME:
            case TIMESTAMP:
                return DataType.TIME_INDICATOR;
            default:
                throw new IllegalArgumentException("unexpected sql type: " + fromType);
        }
    }

    @Override
    public RelDataType toExternalType(DataType fromType) {
        SqlTypeName targetTypeName;
        switch (fromType) {
            case INTEGER:
                targetTypeName = SqlTypeName.INTEGER;
                break;
            case LONG:
                targetTypeName = SqlTypeName.BIGINT;
                break;
            case FLOAT:
                targetTypeName = SqlTypeName.FLOAT;
                break;
            case DOUBLE:
                targetTypeName = SqlTypeName.DOUBLE;
                break;
            case STRING:
                targetTypeName = SqlTypeName.VARCHAR;
                break;
            case BOOLEAN:
                targetTypeName = SqlTypeName.BOOLEAN;
                break;
            case TIME_INDICATOR:
                targetTypeName = SqlTypeName.TIMESTAMP;
                break;
            default:
                throw new IllegalStateException("unrecognizable type: " + fromType);
        }
        return relDataTypeFactory.createTypeWithNullability(relDataTypeFactory.createSqlType(targetTypeName), true);
    }
}
