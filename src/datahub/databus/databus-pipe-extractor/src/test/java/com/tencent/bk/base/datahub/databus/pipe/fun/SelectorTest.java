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

package com.tencent.bk.base.datahub.databus.pipe.fun;

import com.google.common.collect.Lists;
import com.tencent.bk.base.datahub.databus.pipe.Config;
import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class SelectorTest {

    @Test
    public void testExecuteSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/selector/selector-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList(1, 2, 3, 4));
        Assert.assertEquals("[2]", ctx.getValues().toString());

        ctx = new Context();
        parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList(6, 2, 3, 4));
        Assert.assertEquals("[3]", ctx.getValues().toString());

        ctx = new Context();
        parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList(11, 2, 3, 4));
        Assert.assertEquals("[4]", ctx.getValues().toString());
    }

    @Test
    public void testExecuteIteratSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/fun/selector/selector-iterate-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList(1, 2, 3, 4));
        Assert.assertEquals("[[1, null, 1, 1], [1, null, 2, 2], [1, null, 3, 3], [1, null, 4, 4]]",
                ctx.flattenValues(ctx.getSchema(), ctx.getValues()).toString());

        ctx = new Context();
        parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList(6, 2, 3, 4));
        Assert.assertEquals("[[null, 6, 6, 1], [null, 6, 2, 2], [null, 6, 3, 3], [null, 6, 4, 4]]",
                ctx.flattenValues(ctx.getSchema(), ctx.getValues()).toString());

        ctx = new Context();
        parser = Config.parse(ctx, confStr);
        parser.execute(ctx, Lists.newArrayList(11, 2, 3, 4));
        Assert.assertEquals("[[11, null, null, null]]", ctx.flattenValues(ctx.getSchema(), ctx.getValues()).toString());
    }
}
