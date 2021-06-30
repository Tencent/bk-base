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

import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
public class PrizeWheel {

    private static final Logger log = LoggerFactory.getLogger(PrizeWheel.class);
    private LinkedHashMap<String, Integer> weights = new LinkedHashMap<>();
    private Integer weightSum;

    public PrizeWheel(Map<String, Integer> weightMap) {
        if (weightMap.isEmpty()) {
            throw new ConnectException("empty weights in cache");
        }
        AtomicInteger sum = new AtomicInteger(0);
        weightMap.forEach((k, v) -> weights.put(k, sum.addAndGet(v)));
        weightSum = sum.intValue();
    }

    /**
     * 转动赌盘，随机一个随机可用jdbc url
     *
     * @return jdbc url
     */
    public String rotate() {
        int w = new Random().nextInt(weightSum);
        Optional<String> url = weights.keySet().stream().filter(k -> weights.get(k) >= w).findFirst();
        if (url.isPresent()) {
            return url.get();
        }

        throw new ConnectException("not found available url");
    }

}
