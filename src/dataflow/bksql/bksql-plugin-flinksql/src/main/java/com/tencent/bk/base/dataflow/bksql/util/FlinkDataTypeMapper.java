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

import com.tencent.blueking.bksql.util.DataType;
import com.tencent.blueking.bksql.util.DataTypeMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;

public class FlinkDataTypeMapper implements DataTypeMapper<TypeInformation<?>> {
    @Override
    public DataType toBKSqlType(TypeInformation<?> fromType) {
        if (fromType.equals(Types.STRING)) {
            return DataType.STRING;
        } else if (fromType.equals(Types.BYTE)) {
            return DataType.INTEGER;
        } else if (fromType.equals(Types.SHORT)) {
            return DataType.INTEGER;
        } else if (fromType.equals(Types.INT)) {
            return DataType.INTEGER;
        } else if (fromType.equals(Types.LONG)) {
            // fixme big_int(bk) -> long(flink) -> long(bk) may cause type mismatch
            return DataType.LONG;
        } else if (fromType.equals(Types.FLOAT)) {
            return DataType.DOUBLE;
        } else if (fromType.equals(Types.DOUBLE)) {
            return DataType.DOUBLE;
        } else if (fromType.equals(Types.BIG_DEC)) {
            return DataType.DOUBLE;
        } else if (fromType.equals(TimeIndicatorTypeInfo.ROWTIME_INDICATOR())) {
            return DataType.TIME_INDICATOR;
        } else {
            throw new IllegalArgumentException("unexpected flink data type: " + fromType);
        }
    }

    @Override
    public TypeInformation<?> toExternalType(DataType fromType) {
        switch (fromType) {
            case INTEGER:
                return Types.INT;
            case LONG:
                return Types.LONG;
            case FLOAT:
                return Types.FLOAT;
            case DOUBLE:
                return Types.DOUBLE;
            case STRING:
                return Types.STRING;
            case BOOLEAN:
                return Types.BOOLEAN;
            case TIME_INDICATOR:
                return TimeIndicatorTypeInfo.ROWTIME_INDICATOR();
            default:
                throw new IllegalStateException("unrecognizable type: " + fromType);
        }
    }
}
