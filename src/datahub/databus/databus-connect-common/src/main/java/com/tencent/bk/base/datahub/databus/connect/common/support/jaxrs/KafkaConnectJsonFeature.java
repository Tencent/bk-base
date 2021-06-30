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

import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.InternalProperties;
import org.glassfish.jersey.internal.util.PropertiesHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Feature;
import javax.ws.rs.core.FeatureContext;


public class KafkaConnectJsonFeature implements Feature {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectJsonFeature.class);
    private static final String JSON_FEATURE = KafkaConnectJsonFeature.class.getSimpleName();

    @Override
    public boolean configure(final FeatureContext context) {
        try {
            final Configuration config = context.getConfiguration();
            final String jsonFeature = CommonProperties
                    .getValue(config.getProperties(), config.getRuntimeType(), InternalProperties.JSON_FEATURE,
                            JSON_FEATURE, String.class
                    );

            // Other JSON providers registered.
            if (!JSON_FEATURE.equalsIgnoreCase(jsonFeature)) {
                return false;
            }

            // Disable other JSON providers.
            context.property(
                    PropertiesHelper
                            .getPropertyNameForRuntime(InternalProperties.JSON_FEATURE, config.getRuntimeType()),
                    JSON_FEATURE);
        } catch (NoSuchMethodError e) {
            // skip
        }

        return true;
    }
}
