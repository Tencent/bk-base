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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;

public class FlinkSavepoint {

    private static final Logger logger = Logger.getLogger(FlinkSavepoint.class);

    /**
     * get flink save point
     *
     * @param rootPath
     * @return flink save point
     * @throws IOException
     */
    public static String getSavepoint(String rootPath) throws IOException {
        FileStatus[] fileStatuses = HdfsUtil.list(rootPath);
        if (fileStatuses != null && fileStatuses.length > 0) {
            String savepointPath = fileStatuses[0].getPath().toString();
            FileStatus[] savepointfiles = HdfsUtil.list(savepointPath);
            if (savepointfiles == null || savepointfiles.length == 0) {
                logger.error("savepoint path: " + savepointPath + " has no savepoint file. clear the path.");
                HdfsUtil.delete(savepointPath);
                return null;
            } else {
                return savepointPath;
            }
        }
        return null;
    }


    /**
     * delete flink savepoint
     *
     * @param path
     * @throws IOException
     */
    public static void deleteSavepoint(String path) throws IOException {
        if (StringUtils.isNotEmpty(path)) {
            HdfsUtil.delete(path);
        }
    }
}
