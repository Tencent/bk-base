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

package com.tencent.bk.base.dataflow.jobnavi.log;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;

public class LogAggregationUtil {

    private static final Logger LOGGER = Logger.getLogger(LogAggregationUtil.class);

    /**
     * delete expire hdfs path
     *
     * @param path
     * @throws IOException
     */
    public void delete(String path) throws IOException {
        LOGGER.info("call delete hdfs path: " + path);
        HdfsUtil.delete(path);
    }

    /**
     * aggregate local log to hdfs
     *
     * @param localPath
     * @param destPath
     */
    public void aggregateLog(String localPath, String destPath) throws IOException {
        File logFileDir = new File(localPath);
        LOGGER.info("process aggregate log path: " + localPath);
        if (logFileDir.exists()) {
            sendPathToHDFS(logFileDir, destPath);
        }
    }

    public Long getLogFileSize(String hdfsPath) throws IOException {
        return HdfsUtil.getFileSize(hdfsPath);
    }

    public String getLogContent(String hdfsPath) throws IOException {
        return HdfsUtil.readFile(hdfsPath);
    }

    public String getLogContent(String hdfsPath, long begin, long end) throws IOException {
        return HdfsUtil.readFile(hdfsPath, begin, end);
    }

    public String extractLastFromLogFile(String hdfsPath, String regex) throws IOException {
        return HdfsUtil.extractLastFromFile(hdfsPath, regex);
    }

    private void sendPathToHDFS(File logFileDir, String aggregationPath) throws IOException {
        HdfsUtil.mkdirs(aggregationPath);
        File[] logFiles = logFileDir.listFiles();
        if (logFiles != null) {
            for (File logFile : logFiles) {
                HdfsUtil.uploadFileToHdfs(logFile.getCanonicalPath(), aggregationPath);
                if (!logFile.delete()) {
                    LOGGER.warn("log file:" + logFile.getName() + " was not deleted successfully");
                }
            }
        }
        if (!logFileDir.delete()) {
            LOGGER.warn("log file dir:" + logFileDir.getName() + " was not deleted successfully");
        }
        LOGGER.info("log has aggregated, path:" + aggregationPath);
    }
}
