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

package com.tencent.bk.base.datalab.bksql.table;

import com.tencent.bk.base.datalab.bksql.util.DataType;
import java.util.Objects;

public final class ColumnMetadataImpl implements ColumnMetadata {

    private final String columnName;
    private final DataType dataType;
    private final String description;

    public ColumnMetadataImpl(String columnName, DataType dataType, String description) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.description = description;
    }

    public ColumnMetadataImpl(String columnName, DataType dataType) {
        this(columnName, dataType, "");
    }

    public String getColumnName() {
        return columnName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnMetadataImpl that = (ColumnMetadataImpl) o;
        return Objects.equals(columnName, that.columnName)
                && dataType == that.dataType
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, dataType, description);
    }

    @Override
    public String toString() {
        return "ColumnMetadata{"
                + "columnName='" + columnName + '\''
                + ", dataType=" + dataType
                + ", description='" + description + '\''
                + '}';
    }
}

