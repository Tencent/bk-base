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

package com.tencent.bk.base.datahub.databus.connect.common.support.jaxrs;

import org.glassfish.jersey.internal.spi.AutoDiscoverable;

import javax.annotation.Priority;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.FeatureContext;

/**
 * 没有特殊用途，只用于阻止fastjson中FastJsonFeature的生效
 * priority: 必须小于FastJsonAutoDiscoverable的priority（默认-1）
 * AutoDiscoverable加载顺序为priority从小到大
 */
@Priority(AutoDiscoverable.DEFAULT_PRIORITY - 2)
public class KafkaConnectAutoDiscoverable implements AutoDiscoverable {

    @Override
    public void configure(final FeatureContext context) {
        final Configuration config = context.getConfiguration();

        if (!config.isRegistered(KafkaConnectJsonFeature.class)) {
            context.register(KafkaConnectJsonFeature.class);
        }
    }
}
