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

import java.util.concurrent.Callable;
import org.junit.Assert;

public class AssertHelpers {

    private AssertHelpers() {
    }

    public static void assertThrows(String message,
            Class<? extends Exception> expected,
            String containedInMessage,
            Callable callable) {
        try {
            callable.call();
            Assert.fail("No exception was thrown (" + message + "), expected: " +
                    expected.getName());
        } catch (Exception actual) {
            handleException(message, expected, containedInMessage, actual);
        }
    }

    public static void assertThrows(String message,
            Class<? extends Exception> expected,
            String containedInMessage,
            Runnable runnable) {
        try {
            runnable.run();
            Assert.fail("No exception was thrown (" + message + "), expected: " +
                    expected.getName());
        } catch (Exception actual) {
            handleException(message, expected, containedInMessage, actual);
        }
    }

    private static void handleException(String message,
            Class<? extends Exception> expected,
            String containedInMessage,
            Exception actual) {
        try {
            Assert.assertEquals(message, expected, actual.getClass());
            if (actual.getMessage() != null) {
                Assert.assertTrue(
                        "Expected exception message (" + containedInMessage + ") missing: " +
                                actual.getMessage(),
                        actual.getMessage().contains(containedInMessage)
                );
            }
        } catch (AssertionError e) {
            e.addSuppressed(actual);
            throw e;
        }
    }
}