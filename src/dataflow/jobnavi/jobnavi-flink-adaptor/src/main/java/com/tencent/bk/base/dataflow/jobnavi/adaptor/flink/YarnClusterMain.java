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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.flink;

import java.io.File;
import java.util.Arrays;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.log4j.Logger;

public class YarnClusterMain {

    private static final Logger logger = Logger.getLogger(YarnClusterMain.class);

    public static void main(String[] args) {
        try {
            String configPath = args[4];
            String jobName = args[3];
            String[] flinkArg = Arrays.copyOfRange(args, 6, args.length);
            File configDirectory = new File(configPath);
            logger.info("Using configuration directory " + configDirectory.getAbsolutePath());
            Configuration config = GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());
            RunJob runJob = new RunJob();
            runJob.submitYarnCluster(flinkArg, config, configPath, jobName);
            System.out.println(runJob.getApplicationId());
        } catch (Throwable e) {
            logger.error("submit yarn cluster error.", e);
            System.exit(1);
        }
    }
}
