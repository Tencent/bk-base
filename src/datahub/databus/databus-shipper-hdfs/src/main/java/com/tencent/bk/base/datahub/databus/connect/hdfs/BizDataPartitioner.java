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

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BizDataPartitioner {

    private static final Logger log = LoggerFactory.getLogger(BizDataPartitioner.class);

    static final String badRecordPartition = "kafka/badrecord";

    /**
     * 获取partition名称
     *
     * @param tableName 表名
     * @param bizId 业务id
     * @param time 时间(yyyyMMddHHmmss格式)
     * @return partition名称
     */
    public static String encodePartition(String tableName, String bizId, long time) {
        String partition = getPartitionStr(tableName, bizId, time);
        // 当无法从记录中解析出所属partition时，统一写到badrecord目录下
        partition = StringUtils.isBlank(partition) ? badRecordPartition : partition;
        return partition;
    }


    /**
     * 文件存储的分区，按照bizid/tablename/yyyy/MM/dd/HH的目录结构构建，按照小时分区存储。
     * 时间字段从kafka msg的key字段上的时间(yyyyMMddHHmmss格式)中获取。
     *
     * @return 这条数据所属的文件存储路径
     */
    public static String getPartitionStr(String tableName, String bizId, long time) {
        try {
            // 格式 20160818115805 (yyyyMMddHHmmss)
            String str = time + "";
            return bizId + "/" + tableName + "/" + str.substring(0, 4) + "/" + str.substring(4, 6) + "/" + str
                    .substring(6, 8) + "/" + str.substring(8, 10);
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.HDFS_BAD_TIME_STRING,
                    String.format("not a valid time to get hadoop partition path! %s %s %s", tableName, bizId, time),
                    e);
        }
        return "";
    }

    /**
     * 从partition获取日期时间, bizid/tablename/yyyy/MM/dd/HH
     *
     * @param partition 分区路径
     * @return 日期yyyyMMdd
     */
    public static long parseDateFromPartition(String partition) {
        long datetime = -1;
        try {
            String[] strings = partition.split("/");
            datetime = Long.parseLong(strings[2] + strings[3] + strings[4]);
        } catch (Exception e) {
            log.error("can not parse from partition={}", partition);
        }
        return datetime;
    }

}
