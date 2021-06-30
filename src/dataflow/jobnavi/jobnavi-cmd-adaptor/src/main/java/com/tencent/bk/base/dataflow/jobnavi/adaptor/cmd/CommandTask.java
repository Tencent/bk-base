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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.cmd;

import com.tencent.bk.base.dataflow.jobnavi.api.AbstractJobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class CommandTask extends AbstractJobNaviTask {

    @Override
    public void startTask(TaskEvent event) throws Throwable {
        Logger logger = getTaskLogger();
        long executeId = event.getContext().getExecuteInfo().getId();
        String shellCmd = event.getContext().getTaskInfo().getExtraInfo();
        logger.info("execute command: " + shellCmd);
        ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", shellCmd);
        builder.redirectErrorStream(true);
        InputStream is = null;
        BufferedReader reader = null;
        try {
            shellCheck(shellCmd);
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
                throw new NaviException("Process result is not zero, result is: " + exit);
            } else {
                logger.info("execute " + executeId + " process success.");
            }
        } catch (Exception e) {
            logger.error("run shell command error.", e);
            throw e;
        } finally {
            if (is != null) {
                is.close();
            }

            if (reader != null) {
                reader.close();
            }
        }
    }


    private void shellCheck(String command) throws NaviException {
        //TODO use shell checker
        if (StringUtils.contains(command, "rm -rf /")) {
            throw new NaviException("command may delete all files.");
        }
    }

    @Override
    public EventListener getEventListener(String eventName) {
        return null;
    }
}
