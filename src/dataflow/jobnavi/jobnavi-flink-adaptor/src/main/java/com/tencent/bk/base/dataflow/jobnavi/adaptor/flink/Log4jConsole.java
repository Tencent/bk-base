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

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

public class Log4jConsole {

    /**
     * log4j添加System.out输出，需要获取yarn提交日志。
     *
     * @throws IOException
     */
    public static void resetLog4j() throws IOException {
        String log4jFile = System.getProperty("log4j.configuration");
        if (StringUtils.isNotBlank(log4jFile)) {
            Properties props = new Properties();
            props.load(new URL(log4jFile).openStream());
            String rootLogger = props.getProperty("log4j.rootLogger");
            if (StringUtils.isNotBlank(rootLogger)) {
                props.setProperty("log4j.rootLogger", rootLogger + ", yarnAdaptorStdout");
                props.setProperty("log4j.appender.yarnAdaptorStdout", "org.apache.log4j.ConsoleAppender");
                props.setProperty("log4j.appender.yarnAdaptorStdout.Target", "System.out");
                props.setProperty("log4j.appender.yarnAdaptorStdout.layout", "org.apache.log4j.PatternLayout");
                props.setProperty("log4j.appender.yarnAdaptorStdout.layout.ConversionPattern", "%m%n");
                LogManager.resetConfiguration();
                PropertyConfigurator.configure(props);
            }
        }
    }
}
