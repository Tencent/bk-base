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

package com.tencent.bk.base.datalab.bksql.util;

import com.tencent.bk.base.datalab.bksql.exception.ParseException;
import com.tencent.bk.base.datalab.bksql.exception.TokenMgrException;
import java.util.Locale;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocalizedMessageTest {

    private static final Object[] args = new String[]{"P1", "P2"};
    private static final String key = "test.message";

    @Before
    public void setUp() {
        UTF8ResourceBundleControlProvider.enable();
    }

    @Test
    public void testDefaultLocale() {
        String message = new LocalizedMessage(key, args, LocalizedMessage.class, Locale.US)
                .getMessage();
        Assert.assertEquals("Test message, parameters: P1 and P2", message);
    }

    @Test
    public void testPRC() {
        String message = new LocalizedMessage(key, args, LocalizedMessage.class, Locale.PRC)
                .getMessage();
        Assert.assertEquals("测试用消息, 参数: P1 和 P2", message);
    }

    @Test
    public void testParseException() {
        String msgKey = "exception.message";
        Object[] msgArgs = new String[]{"token", "2", "2"};
        String message = new LocalizedMessage(msgKey, msgArgs, ParseException.class, Locale.PRC)
                .getMessage();
        Assert.assertEquals("非法字符：token，位置：第 2 行，第 2 列，建议使用反引号转义或修改sql语句。", message);
    }

    @Test
    public void testTokenMsgException() {
        String msgKey = "exception.message";
        Object[] msgArgs = new String[]{"2", "2"};
        String message = new LocalizedMessage(msgKey, msgArgs, TokenMgrException.class, Locale.PRC)
                .getMessage();
        Assert.assertEquals("无法识别的非法字符，位置：第 2 行，第 2 列。", message);
    }
}