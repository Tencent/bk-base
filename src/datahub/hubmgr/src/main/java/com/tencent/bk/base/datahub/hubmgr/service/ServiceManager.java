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

package com.tencent.bk.base.datahub.hubmgr.service;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServiceManager {

    private static Logger log = LoggerFactory.getLogger(ServiceManager.class);

    // 所有的服务对象，按照注册的先后顺序处理
    private static final Map<String, Service> SERVICES = new LinkedHashMap<>();

    /**
     * 注册服务
     *
     * @param service 服务对象
     */
    public static void registerService(Service service) {
        LogUtils.info(log, "register service {}", service.getServiceName());
        SERVICES.put(service.getServiceName(), service);
    }

    /**
     * 注销服务
     *
     * @param service 服务对象
     */
    public static void unregisterService(Service service) {
        LogUtils.info(log, "unregister service {}", service.getServiceName());
        SERVICES.remove(service.getServiceName());
    }

    /**
     * 启动所有的服务
     *
     * @throws Exception 启动所有服务过程中发生的最后一个异常
     */
    public static void startAll() throws Exception {
        Exception lastException = null;
        for (Map.Entry<String, Service> entry : SERVICES.entrySet()) {
            LogUtils.info(log, "Starting service {}", entry.getKey());
            try {
                entry.getValue().start();
            } catch (Exception e) {
                lastException = e;
                LogUtils.error(MgrConsts.ERRCODE_START_SERVICE, log,
                        String.format("Start service %s failed!", entry.getKey()), e);
            }
        }

        if (lastException != null) {
            throw lastException;
        }

    }

    /**
     * 停止所有的服务
     *
     * @throws Exception 停止所有服务过程中发生的最后一个异常
     */
    public static void stopAll() throws Exception {
        Exception lastException = null;
        for (Map.Entry<String, Service> entry : SERVICES.entrySet()) {
            LogUtils.info(log, "Stopping service {}", entry.getKey());
            try {
                entry.getValue().stop();
            } catch (Exception e) {
                lastException = e;
                LogUtils.error(MgrConsts.ERRCODE_STOP_SERVICE, log,
                        String.format("Stopping service %s failed! ", entry.getKey()), e);
            }
        }

        if (lastException != null) {
            throw lastException;
        }
    }

    /**
     * 获取所有注册过的服务名称
     *
     * @return 所有的服务名称，字符串
     */
    public static String getAllServiceNames() {
        return StringUtils.join(SERVICES.keySet(), ",");
    }
}
