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

package com.tencent.bk.base.dataflow.core.topo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.WindowType;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestMapNodeUtil {

    @Test
    public void testMapWindowConfig() {
        Map<String, Object> windowInfo = ImmutableMap.<String, Object>builder()
                .put("type", "tumbling")
                .put("count_freq", 300)
                .put("lateness_count_freq", 60)
                .put("lateness_time", 1)
                .put("length", 0)
                .put("expired_time", 0)
                .put("allowed_lateness", false)
                .put("waiting_time", 0)
                .build();
        WindowConfig windowConfig = MapNodeUtil.mapWindowConfig(windowInfo);
        Assert.assertEquals(WindowType.tumbling, windowConfig.getWindowType());
        assertEquals(0, windowConfig.getDelay());
        assertEquals(300, windowConfig.getCountFreq());
        assertFalse(windowConfig.isAllowedLateness());
    }

}
