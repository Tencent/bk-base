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

import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.errors.ApiException;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.Assert;


@Slf4j
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class ClickHouseBeanTest {

    /**
     * 覆盖put方法中 startTask 的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testGetWeightMap() throws ApiException {
        Map<String, Object> weights = new HashMap<>();
        weights.put("127.0.0.1:8123", 20);
        weights.put("127.0.0.2:8123", 20);
        weights.put("127.0.0.3:8123", 20);
        Map<String, Object> data = new HashMap<>();
        data.put("timestamp", System.currentTimeMillis());
        data.put("weights", weights);

        ApiResult result = new ApiResult();
        result.setCode("200");
        result.setMessage("OK");
        result.setErrors(null);
        result.setResult(true);
        result.setData(data);
        String weightsUrl = "http://localhost:8080/v3/storekit/clickhouse/100_test/weights/";
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getApiResult(weightsUrl)).thenReturn(result);
        ConcurrentMap<String, Integer> weightMap = ClickHouseBean.getWeightMap(weightsUrl);
        Assert.assertEquals(weightMap.get("jdbc:clickhouse://127.0.0.1:8123"), Integer.valueOf(20));
        Assert.assertEquals(weightMap.get("jdbc:clickhouse://127.0.0.2:8123"), Integer.valueOf(20));
        Assert.assertEquals(weightMap.get("jdbc:clickhouse://127.0.0.3:8123"), Integer.valueOf(20));

        ClickHouseBean bean = new ClickHouseBean();
        bean.setWeightsUrl(weightsUrl).updateWeights("100_test");
        Assert.assertEquals(bean.getWeightMap().get("jdbc:clickhouse://127.0.0.1:8123"), Integer.valueOf(20));
        Assert.assertEquals(bean.getWeightMap().get("jdbc:clickhouse://127.0.0.2:8123"), Integer.valueOf(20));
        Assert.assertEquals(bean.getWeightMap().get("jdbc:clickhouse://127.0.0.3:8123"), Integer.valueOf(20));
        Assert.assertEquals(bean.getWeightMap().size(), 3);
        bean.removeWeight("jdbc:clickhouse://127.0.0.1:8123");
        Assert.assertEquals(bean.getWeightMap().get("jdbc:clickhouse://127.0.0.2:8123"), Integer.valueOf(20));
        Assert.assertEquals(bean.getWeightMap().get("jdbc:clickhouse://127.0.0.3:8123"), Integer.valueOf(20));
        Assert.assertEquals(bean.getWeightMap().size(), 2);

    }


}
