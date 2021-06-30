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

package com.tencent.bk.base.datahub.databus.connect.source.http;

import java.io.UnsupportedEncodingException;
import org.junit.Assert;
import org.junit.Test;

public class UrlUtilsTest {

    @Test
    public void testBuildUrlSuccess() throws UnsupportedEncodingException {
        String baseUrl = "http://xxx/test_update/get_dnf_update_data/?key=testkey";
        String startField = "start";
        String endField = "end";
        String startTime = "2020-02-02 01:01:01";
        String endTime = "2020-02-02 01:01:02";
        String buildUrl = UrlUtils.buildUrl(baseUrl, startField, endField, startTime, endTime);
        String expect = "http://xxx/test_update/get_dnf_update_data/?key=testkey&start=2020-02-02+01%3A01%3A01&end"
                + "=2020-02-02+01%3A01%3A02";
        Assert.assertEquals(expect, buildUrl);
    }

}
