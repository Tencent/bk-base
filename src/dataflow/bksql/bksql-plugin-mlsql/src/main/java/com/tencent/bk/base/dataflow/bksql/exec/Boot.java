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

package com.tencent.bk.base.dataflow.bksql.exec;

import com.beust.jcommander.JCommander;
import com.tencent.bk.base.dataflow.bksql.rest.RestServer;
import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Boot {

    private static final Logger logger = LoggerFactory.getLogger(Boot.class);

    /**
     * 启动Server
     */
    public static void main(String[] args) throws IOException {
        initLogger();
        ArgObject argObject = new ArgObject();
        JCommander jCommander = new JCommander(argObject);
        jCommander.parse(args);
        if (argObject.help) {
            jCommander.usage();
            return;
        }
        final RestServer restServer = new RestServer(new BKSqlService(argObject.configDirectory), argObject.hostname,
                argObject.port, argObject.path);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    restServer.close();
                } catch (Exception e) {
                    logger.error("Error closing rest service: ", e);
                }
            }
        }));
    }

    private static void initLogger() {
//        ch.qos.logback.classic.Logger rootLogger =
//                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
//
//        LoggerContext loggerContext = rootLogger.getLoggerContext();
//        loggerContext.reset();
//
//        RollingFileAppender<ILoggingEvent> rfAppender = new
//                RollingFileAppender<>();
//        rfAppender.setContext(loggerContext);
//        String logFolder = new File(Boot.class.getProtectionDomain()
//                .getCodeSource()
//                .getLocation()
//                .getFile())
//                .getParentFile()
//                .getParentFile()
//                .getAbsolutePath() + File.separator + "logs" + File.separator;
//
//        rfAppender
//                .setFile(logFolder + "bksql.log");
//
//        FixedWindowRollingPolicy fwRollingPolicy = new
//                FixedWindowRollingPolicy();
//        fwRollingPolicy.setContext(loggerContext);
//        fwRollingPolicy.setFileNamePattern(logFolder + "bksql-%i.log.zip");
//        fwRollingPolicy.setParent(rfAppender);
//        fwRollingPolicy.start();
//
//        SizeBasedTriggeringPolicy<ILoggingEvent> triggeringPolicy = new
//                SizeBasedTriggeringPolicy<>();
//        triggeringPolicy.setMaxFileSize(new FileSize(5_000_000L));
//        triggeringPolicy.start();
//
//        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
//        encoder.setContext(loggerContext);
//        encoder.setPattern("%d{yyyy-MM-dd_HH:mm:ss.SSS} [%thread] %-5level %logger{35} - %msg%n");
//        encoder.start();
//
//        rfAppender.setEncoder(encoder);
//        rfAppender.setRollingPolicy(fwRollingPolicy);
//        rfAppender.setTriggeringPolicy(triggeringPolicy);
//        rfAppender.start();
//
//        rootLogger.addAppender(rfAppender);
//
//        StatusPrinter.print(loggerContext);
    }
}
