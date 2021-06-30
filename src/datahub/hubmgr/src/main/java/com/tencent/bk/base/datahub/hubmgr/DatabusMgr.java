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

package com.tencent.bk.base.datahub.hubmgr;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.service.Service;
import com.tencent.bk.base.datahub.hubmgr.service.ServiceManager;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabusMgr {

    private static final Logger log = LoggerFactory.getLogger(DatabusMgr.class);

    private static final String DEFAULT_PACKAGE = "com.tencent.bk.base.datahub.hubmgr.service.";
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    private static CuratorFramework ZK_CLIENT = null;

    /**
     * 获取databus mgr是否还在运行中
     *
     * @return databus mgr服务是否在运行中
     */
    public static boolean isRunning() {
        return RUNNING.get();
    }

    /**
     * 获取zookeeper的客户端。
     *
     * @return zk的客户端
     */
    public static CuratorFramework getZkClient() {
        return ZK_CLIENT;
    }

    /**
     * main方法，用于启动整个manager进程
     *
     * @param args 参数
     * @throws IOException 异常
     */
    public static void main(String[] args) throws IOException {
        LogUtils.info(log, "/**** going to start manager server ****/");
        try {
            // 增加shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // 标记停止databus mgr被触发
                    RUNNING.set(false);
                    LogUtils.info(log, "receive stop signal, stopping all services. {}",
                            ServiceManager.getAllServiceNames());
                    try {
                        ServiceManager.stopAll();
                    } catch (Throwable e) {
                        LogUtils.error(MgrConsts.ERRCODE_STOP_SERVICE, log,
                                "Failed to stop all services during shutdown: ", e);
                    }
                    if (ZK_CLIENT != null) {
                        // 关闭ZK的连接
                        ZK_CLIENT.close();
                    }
                }
            });

            // 加载配置文件，获取需要启动的服务列表
            DatabusProps props = DatabusProps.getInstance();
            BasicProps.getInstance().addProps(props.toMap());
            // 首先初始化zookeeper的client，如果连接zk失败，则退出执行
            String zkAddr = props.getProperty(MgrConsts.DATABUSMGR_ZK_ADDR);
            if (StringUtils.isBlank(zkAddr)) {
                LogUtils.error(MgrConsts.ERRCODE_BAD_CONFIG, log,
                        "empty zk addr config, unable to start manager server!");
                System.exit(1); // 启动失败，退出
            }
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            ZK_CLIENT = CuratorFrameworkFactory.newClient(zkAddr, retryPolicy);
            ZK_CLIENT.start();

            // 获取所需启动的服务列表
            String[] serviceNames = props.getArrayProperty(MgrConsts.DATABUSMGR_SERVICES, ",");

            // 逐个初始化服务对象
            for (String serviceClass : serviceNames) {
                // 这里可以是完整的className，也可以是默认service包中的className简称
                serviceClass = serviceClass.contains(".") ? serviceClass.trim() : DEFAULT_PACKAGE + serviceClass.trim();
                Class clazz = Class.forName(serviceClass.trim());
                Service service = (Service) clazz.newInstance();
                ServiceManager.registerService(service);
            }

            // 启动所有的服务
            ServiceManager.startAll();
        } catch (Throwable e) {
            LogUtils.error(MgrConsts.ERRCODE_STOP_MGRSERVER, log, "start manager server failed! ", e);
            System.exit(1); // 启动发生异常，退出
        }

    }

}
