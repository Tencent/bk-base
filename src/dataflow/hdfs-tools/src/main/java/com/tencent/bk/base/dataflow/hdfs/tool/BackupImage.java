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

package com.tencent.bk.base.dataflow.hdfs.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.jobnavi.api.JobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


public class BackupImage implements JobNaviTask {

    private static final Logger LOGGER = Logger.getLogger(BackupImage.class);

    public void run(TaskEvent taskEvent) throws Throwable {
        String extraInfo = taskEvent.getContext().getTaskInfo().getExtraInfo();
        ObjectMapper objectMapper = new ObjectMapper();
        Map<?, ?> parameters = objectMapper.readValue(extraInfo, HashMap.class);
        String path = parameters.get("path").toString();
        checkBackupPath(path);
        moveOldImageBackup(path);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        final URL infoServer = DFSUtil.getInfoServer(
                HAUtil.getAddressOfActive(fs), conf,
                DFSUtil.getHttpClientScheme(conf)).toURL();
        TransferFsImage.downloadMostRecentImageToDirectory(infoServer, new File(path));
        LOGGER.info("download image success.");
        removeOldImage(path);
    }


    private void checkBackupPath(String path) {
        File file = new File(path);
        if (file.mkdirs()) {
            LOGGER.info("back up path may not exist. do mkdir.");
        }
    }

    private void moveOldImageBackup(String backupDir) {
        File[] files = new File(backupDir).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.renameTo(new File(backupDir + "/" + file.getName() + "_bak"))) {
                    LOGGER.info("move old image to bak : " + backupDir + "/" + file.getName());
                }
            }
        }
    }

    private void removeOldImage(String backupDir) throws IOException {
        File[] files = new File(backupDir).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && isEndWith(file.getName(), "_bak") && file.delete()) {
                    LOGGER.info("remove bak image: " + file.getCanonicalPath());
                }
            }
        }
    }

    private boolean isEndWith(String str, String endStr) {
        int end = str.lastIndexOf(endStr);
        return end == str.length() - endStr.length();
    }

    public EventListener getEventListener(String s) {
        return null;
    }
}
