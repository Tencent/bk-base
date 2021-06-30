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

package com.tencent.bk.base.datahub.databus.pipe.fun.logic;


import com.tencent.bk.base.datahub.databus.pipe.fun.logic.variable.Variable;
import com.tencent.bk.base.datahub.databus.pipe.fun.logic.variable.VariableFactory;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class VariableTest {

    @Test
    public void constantVarTest() {
        // 非必须数值
        Variable var = VariableFactory.createVariable(EtlConsts.LOGIC_EQUAL, 1);
        var.initVal("");
        Assert.assertTrue(var.getVal().equals("1"));

        var = VariableFactory.createVariable(EtlConsts.LOGIC_EQUAL, "1");
        var.initVal("");
        Assert.assertTrue(var.getVal().equals("1"));

        // 必须数值
        var = VariableFactory.createVariable(EtlConsts.LOGIC_GREATER, 1);
        var.initVal("");
        Assert.assertTrue(var.getVal().equals(1));

        var = VariableFactory.createVariable(EtlConsts.LOGIC_GREATER, "1");
        var.initVal("");
        Assert.assertEquals(1L ,var.getVal());

        var = VariableFactory.createVariable(EtlConsts.LOGIC_GREATER, "1.5");
        var.initVal("");
        Assert.assertTrue((Double) var.getVal() == 1.5);

        var = VariableFactory.createVariable(EtlConsts.LOGIC_GREATER, "1.5.1");
        Assert.assertTrue(var == null);

        var = VariableFactory.createVariable(EtlConsts.LOGIC_GREATER, "a");
        Assert.assertTrue(var == null);
    }

    @Test
    public void objVarTest() {
        Variable var = VariableFactory.createVariable(EtlConsts.LOGIC_EQUAL, "$ALL");
        var.initVal("test");
        Assert.assertTrue(var.getVal().equals("test"));
    }

    @Test
    public void indexVarTest() {
        Variable var = VariableFactory.createVariable(EtlConsts.LOGIC_EQUAL, "$0");
        var.initVal(Arrays.asList("test"));
        Assert.assertTrue(var.getVal().equals("test"));
    }

    @Test
    public void keyVarTest() {
        Variable var = VariableFactory.createVariable(EtlConsts.LOGIC_EQUAL, "$a");
        Map<String, String> map = new HashMap<>();
        map.put("a", "test");
        var.initVal(map);
        Assert.assertTrue(var.getVal().equals("test"));
    }
}
