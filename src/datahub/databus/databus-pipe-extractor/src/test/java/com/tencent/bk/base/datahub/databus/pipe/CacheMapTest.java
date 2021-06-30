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

package com.tencent.bk.base.datahub.databus.pipe;


import com.tencent.bk.base.datahub.databus.pipe.utils.JdbcUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class CacheMapTest {

    private Map<String, Map<String, String>> props;

    @Before
    public void setUp() {
        props = new HashMap<>();
        Map<String, String> cacheMapConf = new HashMap<>();
        cacheMapConf.put(EtlConsts.HOST, "xxx");
        cacheMapConf.put(EtlConsts.PORT, "3306");
        cacheMapConf.put(EtlConsts.USER, "user");
        cacheMapConf.put(EtlConsts.PASSWD, "pass");
        cacheMapConf.put(EtlConsts.DB, "db");
        props.put(EtlConsts.CC_CACHE, cacheMapConf);

        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.when(JdbcUtils.getCache("jdbc:mysql://xxx:3306/", "user", "pass", "db", "tb", "map", "key", "|"))
                .thenReturn(map);
    }

    /**
     * 基础功能
     * 外层数据缺失异常测试
     */
    @Test
    @PrepareForTest(JdbcUtils.class)
    public void testCacheMapFun() throws Exception {
        String confStr = TestUtils.getFileContent("/adhoc_test.conf");
        ETL etl = new ETLImpl(confStr, props);
        String data = TestUtils.getFileContent("/adhoc_test.data");
        try {
            ETLResult ret = etl.handle(data.getBytes());
        } catch (Exception e) {
        }

    }

}
