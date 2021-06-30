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

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.logging.ThreadLoggingFactory;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Logger;

public class CliCommandV2 {

    private static final Logger logger = Logger.getLogger(CliCommandV2.class);

    /**
     * 通过脚本启动 flink 任务
     *
     * @param flinkArg flink 任务需要的参数
     * @param execId execute id
     * @param env 适配层对应的env路径
     * @param type 适配层对应的type
     * @throws Exception 提交任务异常
     */
    public static void run(String flinkArg, Long execId, String env, String type) throws Exception {
        String path = System.getProperty("JOBNAVI_HOME");
        String shellPath = path + "/adaptor/" + type + "/bin/flink_command_v2.sh";
        String rootLogPath = ThreadLoggingFactory.getLoggerRootPath();
        String startCommand = shellPath + " " + execId
                + " " + env + " " + rootLogPath + " \"" + flinkArg + "\"";
        logger.info(startCommand);
        BufferedReader reader = null;
        InputStream is = null;
        try {
            ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", startCommand);
            Process process = builder.start();
            is = process.getInputStream();
            reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                logger.info(line);
            }
            process.waitFor();
            int exit = process.exitValue();
            if (exit != 0) {
                throw new NaviException("Command " + flinkArg + " error.");
            }
        } finally {
            if (is != null) {
                is.close();
            }
            if (reader != null) {
                reader.close();
            }
        }
    }
}
