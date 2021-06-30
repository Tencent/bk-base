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

package com.tencent.bk.base.datahub.databus.connect.common.cli;


import com.tencent.bk.base.datahub.databus.commons.utils.LicenseChecker;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 包装一下common包中的启动类，将这个类作为程序的启动入口
 */
public class BkDataDatabusShipper {

    private static final Logger log = LoggerFactory.getLogger(BkDataDatabusShipper.class);

    /**
     * 程序入口
     *
     * @param args 启动参数
     * @throws Exception 异常
     */
    public static void main(String[] args) throws Exception {
        String workerPropsFile = args[0];
        Map<String, String> workerProps =
                !workerPropsFile.isEmpty() ? Utils.propsToStringMap(Utils.loadProps(workerPropsFile))
                        : Collections.<String, String>emptyMap();

        String certFile = workerProps.get("cert.file");
        String certServer = "https://" + workerProps.get("cert.server") + "/certificate";
        String platform = workerProps.getOrDefault("platform", "bkdata");
        if (!LicenseChecker.checkLicense(certFile, certServer, platform)) {
            try {
                Thread.sleep(3000); // 增加sleep时间，避免检查license失败后，频繁被supervisor拉起
            } catch (InterruptedException ignore) {
                // ignore
            }
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CERT_ERR, log,
                    "bad license... unable to start the bkdata server");
            throw new ConnectException("bad license... unable to start the bkdata server");
        } else {
            LogUtils.info(log, "valid license, go!");
            // 周期性检查证书，当证书无效时，停止服务
            ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
            exec.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    if (LicenseChecker.checkLicense(certFile, certServer, platform)) {
                        LogUtils.info(log, "License is valid!");
                    } else {
                        LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CERT_ERR, log,
                                "License validation failed, going to stop!");
                        System.exit(1);
                    }
                }
            }, 1, 24, TimeUnit.HOURS); // 每天检查一次
            BkDatabusWorker.main(args);
        }
    }
}
