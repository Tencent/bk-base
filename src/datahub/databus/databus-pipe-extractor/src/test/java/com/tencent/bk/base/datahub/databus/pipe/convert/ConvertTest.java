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

package com.tencent.bk.base.datahub.databus.pipe.convert;

import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConvertTest {

    /**
     * 测试BuildConvert方法
     */
    @Test(expected = RuntimeException.class)
    public void testBuildConvert1() {
        Map<String, Object> map = new HashMap<>();
        map.put("type", "fun");
        map.put("method", "xxx");
        Convert.buildConvert(map);
    }

    /**
     * 测试BuildConvert方法
     */
    @Test(expected = RuntimeException.class)
    public void testBuildConvert2() {
        Map<String, Object> map = new HashMap<>();
        map.put("type", "xxx");
        Convert.buildConvert(map);
    }

    /**
     * 测试execute方法
     *
     * @throws Exception
     */
    @Test
    public void testExecute() throws Exception {
        List<Object> list = new ArrayList<>();
        List<Object> result = new ArrayList<>();
        Convert convert = new Convert();
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        result = convert.execute(new ETLImpl(confStr), list);
        Assert.assertEquals(result, null);
    }
}
