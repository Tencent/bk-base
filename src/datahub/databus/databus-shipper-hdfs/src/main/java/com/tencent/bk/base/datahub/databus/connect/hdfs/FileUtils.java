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


package com.tencent.bk.base.datahub.databus.connect.hdfs;

import com.tencent.bk.base.datahub.databus.connect.hdfs.storage.Storage;
import org.apache.hadoop.fs.FileStatus;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;


public class FileUtils {

    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    /**
     * 获取log文件名称
     *
     * @param url hdfs url
     * @param logsDir log目录
     * @param topicPart topic分区
     * @return log文件名称
     */
    public static String logFileName(String url, String logsDir, TopicPartition topicPart) {
        return fileName(url, logsDir, topicPart, "log");
    }

    /**
     * 获取文件名称
     *
     * @param url hdfs url
     * @param topicsDir topics目录
     * @param topicPart topic分区
     * @param name 名称
     * @return 文件名称
     */
    public static String fileName(String url, String topicsDir, TopicPartition topicPart, String name) {
        String topic = topicPart.topic();
        int partition = topicPart.partition();
        return url + "/" + topicsDir + "/" + topic + "/" + partition + "/" + name;
    }

    /**
     * 获取文件名称
     *
     * @param url hdfs url
     * @param topicsDir topics目录
     * @param directory 目录
     * @param name 名称
     * @return 文件名称
     */
    public static String fileName(String url, String topicsDir, String directory, String name) {
        return url + "/" + topicsDir + "/" + directory + "/" + name;
    }

    /**
     * 获取目录名称
     *
     * @param url hdfs url
     * @param topicsDir topics目录
     * @param directory 目录
     * @return 目录名称
     */
    public static String directoryName(String url, String topicsDir, String directory) {
        return url + "/" + topicsDir + "/" + directory;
    }

    /**
     * 获取临时文件名称
     *
     * @param url hdfs url
     * @param topicsDir topics目录
     * @param directory 目录
     * @param extension 后缀名称
     * @return 临时文件名称
     */
    public static String tempFileName(String url, String topicsDir, String directory, String extension) {
        UUID id = UUID.randomUUID();
        String name = id.toString() + "_" + "tmp" + extension;
        return fileName(url, topicsDir, directory, name);
    }

    /**
     * 提交的文件名称
     *
     * @param url hdfs url
     * @param topicsDir topics目录
     * @param directory 目录
     * @param topicPart topic分区
     * @param startOffset 开始offset
     * @param endOffset 结束offset
     * @param extension 后缀名称
     * @param zeroPadFormat 格式化+0
     * @return 提交的文件名称
     */
    public static String committedFileName(String url, String topicsDir, String directory,
            TopicPartition topicPart, long startOffset, long endOffset,
            String extension, String zeroPadFormat) {
        String topic = topicPart.topic();
        int partition = topicPart.partition();
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(HdfsConsts.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(partition);
        sb.append(HdfsConsts.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(String.format(zeroPadFormat, startOffset));
        sb.append(HdfsConsts.COMMMITTED_FILENAME_SEPARATOR);
        sb.append(String.format(zeroPadFormat, endOffset));
        sb.append(extension);

        return fileName(url, topicsDir, directory, sb.toString());
    }

    /**
     * 清理offset记录文件
     *
     * @param storage 存储对象
     * @param nowOffset 当前offset
     * @param dirPath 目录路径
     * @throws IOException 异常
     */
    public static void cleanOffsetRecordFiles(Storage storage, long nowOffset, String dirPath) throws IOException {
        FileStatus[] statuses = storage.listStatus(dirPath);

        for (FileStatus status : statuses) {
            long offset = Long.parseLong(status.getPath().getName());
            if (offset < nowOffset) {
                storage.delete(status.getPath().toString());
            }
        }
    }

    /**
     * 获取最大的offset值
     *
     * @param storage 存储对象
     * @param dirPath 目录路径
     * @return 当前目录中最大的offset值
     * @throws IOException 异常
     */
    public static long getMaxOffsetFromRecordDir(Storage storage, String dirPath) throws IOException {
        FileStatus[] statuses = storage.listStatus(dirPath);
        long max = -1L;
        for (FileStatus status : statuses) {
            long offset = Long.parseLong(status.getPath().getName());
            max = max > offset ? max : offset;
        }
        if (max != -1L) {
            return max + 1;
        } else {
            return max;
        }
    }

    /**
     * 获取offset存储的目录
     *
     * @param url hdfs url
     * @param topicsDir topics目录
     * @param cluster 集群
     * @param connector connector名称
     * @param tp topic分区
     * @return 存储offset数据的目录
     */
    public static String getOffsetRecordDir(String url, String topicsDir, String cluster, String connector,
            TopicPartition tp) {
        String[] arr = cluster.split("-");
        if (arr.length == 3) {
            // 对于内部版，集群名称类似  hdfs-inner2-M  ，这里取inner2作为offset目录的一部分，避免rt被删除再建时，kafka集群发生变化
            return String.format("%s/%s/%s/__offset__/%s/%s", url, topicsDir, arr[1], connector, tp.partition());
        } else if (arr.length == 4) {
            // 对于海外版，集群名称类似  hdfs-inner-NA-M  ，这里取inner-NA作为offset目录的一部分，避免rt被删除再建时，kafka集群发生变化
            return String
                    .format("%s/%s/%s-%s/__offset__/%s/%s", url, topicsDir, arr[1], arr[2], connector, tp.partition());
        } else {
            return String.format("%s/%s/%s/__offset__/%s/%s", url, topicsDir, cluster, connector, tp.partition());
        }
    }

    /**
     * 获取offset存储的目录
     *
     * @param url hdfs url
     * @param topicsDir topics目录
     * @param cluster 集群
     * @param connector connector名称
     * @param tp topic分区
     * @return 存储offset数据的目录，旧版
     */
    public static String getOldOffsetRecordDir(String url, String topicsDir, String cluster, String connector,
            TopicPartition tp) {
        // 旧的offset目录格式
        return url + "/" + topicsDir + "/__offset__/hdfs/" + connector + "/" + tp.topic() + "/" + tp.partition();
    }


}
