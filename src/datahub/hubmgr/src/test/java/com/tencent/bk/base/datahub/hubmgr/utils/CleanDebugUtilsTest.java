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

package com.tencent.bk.base.datahub.hubmgr.utils;

import org.junit.Test;

public class CleanDebugUtilsTest {

    @Test
    public void testCleanDebugSimple() throws Exception {
        String conf = "{\"args\": [], \"next\": {\"subtype\": \"assign_obj\", \"next\": null, \"type\": \"assign\", "
                + "\"assign\": [{\"assign_to\": \"col1\", \"type\": \"string\", \"key\": \"col1\"}, "
                + "{\"assign_to\": \"col2\", \"type\": \"string\", \"key\": \"col2\"}, "
                + "{\"assign_to\": \"time\", \"type\": \"string\", \"key\": \"time\"}], \"label\": \"label021376\"}, "
                + "\"result\": \"dd\", \"label\": \"label15a4c3\", \"type\": \"fun\", \"method\": \"from_json\"}";
        String msg = "{\"col1\": \"val2-1\", \"col2\": \"val2-2\", \"time\": \"2020-03-12 12:00:00\"}";
        String expectedResult = "{\"nodes\":{"
                + "\"label15a4c3\":{\"time\":\"2020-03-12 12:00:00\",\"col2\":\"val2-2\",\"col1\":\"val2-1\"},"
                + "\"label021376\":{\"time\":\"2020-03-12 12:00:00\",\"col2\":\"val2-2\",\"col1\":\"val2-1\"}},"
                + "\"errors\":{},\"error_message\":\"\",\"output_type\":{"
                + "\"label15a4c3\":{\"type\":\"HashMap\",\"value\":{"
                + "\"time\":\"2020-03-12 12:00:00\",\"col2\":\"val2-2\",\"col1\":\"val2-1\"}}},"
                + "\"result\":[[\"val2-1\",\"val2-2\",\"2020-03-12 12:00:00\"]],"
                + "\"schema\":[\"col1(string)\",\"col2(string)\",\"time(string)\"],"
                + "\"display\":[\"user_field\",\"user_field\",\"user_field\"]}";
        String result = CleanDebugUtils.verify(conf, msg);
        assert expectedResult.equals(result);
    }

}
