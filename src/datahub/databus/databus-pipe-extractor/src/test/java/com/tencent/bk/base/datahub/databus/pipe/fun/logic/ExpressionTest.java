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

import com.tencent.bk.base.datahub.databus.pipe.fun.logic.expression.Expression;
import com.tencent.bk.base.datahub.databus.pipe.fun.logic.expression.ExpressionFactory;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class ExpressionTest {

    @Test
    public void equalsExprTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.LOGIC_EQUAL);
        map.put(EtlConsts.LVAL, "1");
        map.put(EtlConsts.RVAL, "1");
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        map.put(EtlConsts.RVAL, 1);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        map.put(EtlConsts.RVAL, "$a");
        map.put("a", 1);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(map));

        map.put("a", 2);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(map));
    }

    @Test
    public void notEqualsExprTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.LOGIC_NOT_EQUAL);
        map.put(EtlConsts.LVAL, "1");
        map.put(EtlConsts.RVAL, "1");
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));

        map.put(EtlConsts.RVAL, 1);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));

        map.put(EtlConsts.RVAL, "$a");
        map.put("a", 1);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(map));

        map.put("a", 2);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(map));
    }

    @Test
    public void containsExprTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.LOGIC_CONTAINS);
        map.put(EtlConsts.LVAL, "test1");
        map.put(EtlConsts.RVAL, "est");
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        map.put(EtlConsts.LVAL, "test1");
        map.put(EtlConsts.RVAL, "aest");
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));
    }

    @Test
    public void startsWithExprTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.LOGIC_STARTSWITH);
        map.put(EtlConsts.LVAL, "test1");
        map.put(EtlConsts.RVAL, "test");
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        map.put(EtlConsts.LVAL, "test1");
        map.put(EtlConsts.RVAL, "aest");
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));
    }

    @Test
    public void endsWithExprTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.LOGIC_ENDSWITH);
        map.put(EtlConsts.LVAL, "test1");
        map.put(EtlConsts.RVAL, "est1");
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        map.put(EtlConsts.LVAL, "test1");
        map.put(EtlConsts.RVAL, "aest");
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));
    }

    @Test
    public void greaterExprTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.LOGIC_GREATER);
        map.put(EtlConsts.LVAL, 12);
        map.put(EtlConsts.RVAL, 2);
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        map.put(EtlConsts.LVAL, 1);
        map.put(EtlConsts.RVAL, 2);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));

        map.put(EtlConsts.LVAL, 1);
        map.put(EtlConsts.RVAL, 1);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));
    }

    @Test
    public void notGreaterExprTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.LOGIC_NOT_GREATER);
        map.put(EtlConsts.LVAL, 1);
        map.put(EtlConsts.RVAL, 2);
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        map.put(EtlConsts.LVAL, 12);
        map.put(EtlConsts.RVAL, 2);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));

        map.put(EtlConsts.LVAL, 1);
        map.put(EtlConsts.RVAL, 1);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));
    }

    @Test
    public void lessExprTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.LOGIC_LESS);
        map.put(EtlConsts.LVAL, 1);
        map.put(EtlConsts.RVAL, 2);
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        map.put(EtlConsts.LVAL, 12);
        map.put(EtlConsts.RVAL, 2);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));

        map.put(EtlConsts.LVAL, 1);
        map.put(EtlConsts.RVAL, 1);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));
    }

    @Test
    public void notLessExprTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.LOGIC_NOT_LESS);
        map.put(EtlConsts.LVAL, 12);
        map.put(EtlConsts.RVAL, 2);
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        map.put(EtlConsts.LVAL, 1);
        map.put(EtlConsts.RVAL, 2);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));

        map.put(EtlConsts.LVAL, 1);
        map.put(EtlConsts.RVAL, 1);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));
    }

    @Test
    public void andExprTest() {
        Map<String, Object> lmap = new HashMap<>();
        lmap.put(EtlConsts.OP, EtlConsts.LOGIC_LESS);
        lmap.put(EtlConsts.LVAL, 1);
        lmap.put(EtlConsts.RVAL, 2);

        Map<String, Object> rmap = new HashMap<>();
        rmap.put(EtlConsts.OP, EtlConsts.LOGIC_LESS);
        rmap.put(EtlConsts.LVAL, 1);
        rmap.put(EtlConsts.RVAL, 2);

        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.AND);
        map.put(EtlConsts.LVAL, lmap);
        map.put(EtlConsts.RVAL, rmap);
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        rmap.put(EtlConsts.LVAL, 10);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));

        lmap.put(EtlConsts.LVAL, 10);
        rmap.put(EtlConsts.LVAL, 10);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));
    }

    @Test
    public void orExprTest() {
        Map<String, Object> lmap = new HashMap<>();
        lmap.put(EtlConsts.OP, EtlConsts.LOGIC_LESS);
        lmap.put(EtlConsts.LVAL, 1);
        lmap.put(EtlConsts.RVAL, 2);

        Map<String, Object> rmap = new HashMap<>();
        rmap.put(EtlConsts.OP, EtlConsts.LOGIC_LESS);
        rmap.put(EtlConsts.LVAL, 1);
        rmap.put(EtlConsts.RVAL, 2);

        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.OR);
        map.put(EtlConsts.LVAL, lmap);
        map.put(EtlConsts.RVAL, rmap);
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        rmap.put(EtlConsts.LVAL, 10);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));

        lmap.put(EtlConsts.LVAL, 10);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));
    }

    @Test
    public void notExprTest() {
        Map<String, Object> lmap = new HashMap<>();
        lmap.put(EtlConsts.OP, EtlConsts.LOGIC_LESS);
        lmap.put(EtlConsts.LVAL, 1);
        lmap.put(EtlConsts.RVAL, 2);

        Map<String, Object> map = new HashMap<>();
        map.put(EtlConsts.OP, EtlConsts.NOT);
        map.put(EtlConsts.LVAL, lmap);
        Expression exp = ExpressionFactory.createExpression(map);
        Assert.assertFalse(exp.execute(""));

        lmap.put(EtlConsts.LVAL, 10);
        exp = ExpressionFactory.createExpression(map);
        Assert.assertTrue(exp.execute(""));
    }
}
