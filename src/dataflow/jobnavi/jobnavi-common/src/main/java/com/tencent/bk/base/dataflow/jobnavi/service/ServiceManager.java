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

package com.tencent.bk.base.dataflow.jobnavi.service;

import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;

public class ServiceManager {

    private static Logger logger = Logger.getLogger(ServiceManager.class);

    private static Map<String, Service> services = new LinkedHashMap<>();

    public static synchronized void registerService(Service service) {
        services.put(service.getServiceName(), service);
    }

    public static synchronized void unregisterService(String serviceName) {
        services.remove(serviceName);
    }

    /**
     * Start All Service
     *
     * @param conf service config
     * @throws NaviException start service error
     */
    public static void startAllService(Configuration conf) throws NaviException {
        for (Entry<String, Service> entry : services.entrySet()) {
            logger.info("Starting service " + entry.getKey() + "...");
            entry.getValue().start(conf);
        }
    }

    /**
     * recovery All Service
     *
     * @param conf service config
     * @throws NaviException recovery service error
     */
    public static void recoveryAllService(Configuration conf) throws NaviException {
        for (Entry<String, Service> entry : services.entrySet()) {
            logger.info("recovery service " + entry.getKey() + "...");
            entry.getValue().recovery(conf);
        }
    }

    /**
     * Stop All Service
     *
     * @param conf service config
     * @throws NaviException stop service error
     */
    public static void stopAllService(Configuration conf) throws NaviException {
        for (Entry<String, Service> entry : services.entrySet()) {
            logger.info("Stopping service " + entry.getKey() + "...");
            entry.getValue().stop(conf);
        }
    }

}
