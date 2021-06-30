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

package com.tencent.bk.base.datahub.iceberg.parser;

import static com.tencent.bk.base.datahub.iceberg.C.DTET;
import static com.tencent.bk.base.datahub.iceberg.C.DTETS;
import static com.tencent.bk.base.datahub.iceberg.C.ET;
import static com.tencent.bk.base.datahub.iceberg.C.LOCALTIME;
import static com.tencent.bk.base.datahub.iceberg.C.THEDATE;

import com.tencent.bk.base.datahub.iceberg.TableField;
import com.tencent.bk.base.datahub.iceberg.Utils;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.datanucleus.util.Base64;
import org.junit.Assert;
import org.junit.Test;

public class TestInnerMsgParser {

    @Test
    public void testParser() {
        String id = "game_event";
        String key = "20200522095841";
        String s64 = Base64.encodeString("abc");
        String[][] arr = new String[][]{
                {ET, "timestamp"},
                {DTET, "string"},
                {DTETS, "long"},
                {THEDATE, "int"},
                {LOCALTIME, "string"},
                {"vClientIp", "string"},
                {"dtEventType", "string"},
                {"LogoutTime", "string"},
                {"gameID", "string"},
                {"iTierID", "string"},
                {"vPreferredMAC", "string"},
                {"iShardID", "string"},
                {"iCafeID", "text"}};

        List<TableField> fields = Stream.of(arr).map(a -> new TableField(a[0], a[1]))
                .collect(Collectors.toList());
        Schema schema = Utils.buildSchema(fields);
        String[] fieldsInOrder = Stream.of(arr).map(a -> a[0]).toArray(String[]::new);

        InnerMsgParser parser = new InnerMsgParser(id, fieldsInOrder, schema);
        Optional<ParseResult> result = parser.parse(key, s64);
        Assert.assertFalse("decode avro message failed, empty result", result.isPresent());

        // test databus event
        String databusKey = "DatabusEvent2020";
        String databusValue = "";
        result = parser.parse(databusKey, databusValue);
        Assert.assertFalse("decode databus event, empty result", result.isPresent());
    }
}