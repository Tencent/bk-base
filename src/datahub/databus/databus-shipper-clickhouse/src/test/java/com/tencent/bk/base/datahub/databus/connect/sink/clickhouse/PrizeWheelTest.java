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


package com.tencent.bk.base.datahub.databus.connect.sink.clickhouse;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;


@Slf4j
@RunWith(PowerMockRunner.class)
public class PrizeWheelTest {

    @Test
    public void testRotate() {
        Map<String, Integer> m = new HashMap<>();
        m.put("a", 20);
        m.put("b", 20);
        m.put("c", 20);
        PrizeWheel p = new PrizeWheel(m);
        Assert.assertEquals(Integer.valueOf(60), p.getWeightSum());
        Assert.assertTrue(p.getWeights().containsValue(20));
        Assert.assertTrue(p.getWeights().containsValue(40));
        Assert.assertTrue(p.getWeights().containsValue(60));

        Map<String, Integer> counts = new HashMap<>();
        counts.put("a", 0);
        counts.put("b", 0);
        counts.put("c", 0);
        for (int i = 0; i < 10000; i++) {
            String r = p.rotate();
            counts.replace(r, counts.get(r) + 1);
        }
        float sum = (float) (counts.get("a") + counts.get("b") + counts.get("c"));
        float a = (float) counts.get("a") / sum;
        float b = (float) counts.get("b") / sum;
        float c = (float) counts.get("c") / sum;
        log.info("probability distribution: a -> {}, b -> {}, c -> {}", a, b, c);
        log.info("count distribution: a -> {}, b -> {}, c -> {}", counts.get("a"), counts.get("c"), counts.get("c"));
        Assert.assertEquals(10000, counts.get("a") + counts.get("b") + counts.get("c"));
        Assert.assertTrue(a > 0.3 && a < 0.4);
        Assert.assertTrue(b > 0.3 && b < 0.4);
        Assert.assertTrue(c > 0.3 && c < 0.4);
    }

}
