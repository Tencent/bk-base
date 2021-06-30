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

package com.tencent.bk.base.datahub.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.RecordWrapper;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestRecordWrapper {

    @Test
    public void testObject() {
        Schema schema = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "data", Types.StringType.get()),
                optional(3, "ts1", Types.TimestampType.withZone()),
                optional(4, "ts2", Types.TimestampType.withoutZone()),
                optional(5, "dt", Types.DateType.get()),
                optional(6, "time", Types.TimeType.get()));

        RecordWrapper wrapper = new RecordWrapper(schema.asStruct());
        Record record = RandomGenericData.generate(schema, 1, 0L).get(0);
        wrapper.wrap(record);

        Assert.assertEquals(record.size(), wrapper.size());
        AssertHelpers.assertThrows("can't update the wrapper",
                UnsupportedOperationException.class,
                "Cannot update InternalRecordWrapper",
                () -> wrapper.set(0, 1));

    }
}