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
import java.text.MessageFormat;
import java.util.Arrays;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.log4j.Logger;

public class RunJobMain {

    private static final Logger logger = Logger.getLogger(RunJobMain.class);

    /**
     * main entry of running flink job
     * @param args
     */
    public static void main(String[] args) {
        Throwable e0 = null;
        String jobJsonFile = null;
        try {
            String configPath = args[4];
            File configDirectory = new File(configPath);
            logger.info("Using configuration directory " + configDirectory.getAbsolutePath());
            Configuration config = GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());
            String[] flinkArg = Arrays.copyOfRange(args, 6, args.length);
            if (flinkArg.length >= 8) {
                jobJsonFile = flinkArg[7];
            }
            RunJob run = new RunJob();
            JobSubmissionResult submitResult = run.submit(flinkArg, config, configPath);
            String jobId = submitResult.getJobID().toString();
            logger.info("Job has been submitted with JobID " + jobId);
            System.out.println(jobId);
        } catch (Throwable e) {
            String jobName = null;
            if (null != jobJsonFile) {
                jobName = jobJsonFile.replace("/tmp/flink-", "").replace(".json", "");
            }
            logger.error(MessageFormat.format("run {0}_error", jobName), e);
            e0 = e;
        } finally {
            if (null != jobJsonFile) {
                File jsonFile = new File(jobJsonFile);
                logger.info("to delete " + jobJsonFile);
                jsonFile.deleteOnExit();
            }
            if (null != e0) {
                System.exit(1);
            }
        }
    }
}
