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

package com.tencent.bk.base.datahub.databus.connect.source.hdfs.bean;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class DataFile {

    /**
     * 当前所在目录
     */
    private Path parentPath;
    /**
     * 文件的路径
     */
    private Path filePath;

    private FileStatus status;
    /**
     * 上一次读取的行号
     */
    private int lastLineNum = -1;

    /**
     * 当前所在目录的修改时间
     */
    private long parentModificationTime;

    public DataFile(Path parentPath, FileStatus status, long parentModificationTime) {
        this.parentPath = parentPath;
        this.filePath = status.getPath();
        this.status = status;
        this.parentModificationTime = parentModificationTime;
    }

    public String getName() {
        return filePath.getName();
    }

    public long getParentModificationTime() {
        return parentModificationTime;
    }

    public Path getFilePath() {
        return filePath;
    }

    public int getLastLineNum() {
        return lastLineNum;
    }

    public void setLastLineNum(int lastLineNum) {
        this.lastLineNum = lastLineNum;
    }

    public Path getParentPath() {
        return parentPath;
    }

    @Override
    public String toString() {
        return status.toString();
    }
}

