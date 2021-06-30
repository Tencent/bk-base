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

package com.tencent.bk.base.datahub.iceberg.errors;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.Assert;
import org.junit.Test;

public class TestErrors {

    @Test
    public void testCreate() {
        CommitFileFailed e1 = new CommitFileFailed("commit failed");
        Assert.assertEquals("error message should equals", "commit failed", e1.getMessage());
        e1 = new CommitFileFailed("commit failed %s %d times", "with", 5);
        Assert.assertEquals("error message should equals", "commit failed with 5 times",
                e1.getMessage());
        e1 = new CommitFileFailed(new RuntimeException("Null Pointer"), "commit failed");
        Assert.assertTrue("root cause should be RuntimeException",
                e1.getCause() instanceof RuntimeException);

        ModifyTableFailed e2 = new ModifyTableFailed("modify failed");
        Assert.assertEquals("error message should equals", "modify failed", e2.getMessage());
        e2 = new ModifyTableFailed("modify failed %s %s", "because", "concurrent commit");
        Assert.assertEquals("error message should equals",
                "modify failed because concurrent commit", e2.getMessage());
        e2 = new ModifyTableFailed(new NullPointerException(), "modify failed");
        Assert.assertTrue("root cause should be NullPointerException",
                e2.getCause() instanceof NullPointerException);

        TableExists e3 = new TableExists("table exists");
        Assert.assertEquals("error message should equals", "table exists", e3.getMessage());
        e3 = new TableExists("table %s already exists", "abc.aaa");
        Assert.assertEquals("error message should equals", "table abc.aaa already exists",
                e3.getMessage());
        e3 = new TableExists(new AlreadyExistsException(""), "table exists");
        Assert.assertTrue("root cause should be AlreadyExistsException",
                e3.getCause() instanceof AlreadyExistsException);

        TableNotExists e4 = new TableNotExists("table not exists");
        Assert.assertEquals("error message should equals", "table not exists", e4.getMessage());
        e4 = new TableNotExists("table %s not exists", "abc.aaa");
        Assert.assertEquals("error message should equals", "table abc.aaa not exists",
                e4.getMessage());
        e4 = new TableNotExists(new NoSuchTableException(""), "table not exists");
        Assert.assertTrue("root cause should be NoSuchTableException",
                e4.getCause() instanceof NoSuchTableException);

    }
}