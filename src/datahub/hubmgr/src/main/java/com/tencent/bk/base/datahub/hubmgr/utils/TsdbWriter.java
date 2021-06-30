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

package com.tencent.bk.base.datahub.hubmgr.utils;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.InfluxdbUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.KeyGen;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TsdbWriter {

    public static final String METRIC_TSDB_OPTARGS_PREFIX = "metric.tsdb.optargs.";

    private static final Logger log = LoggerFactory.getLogger(TsdbWriter.class);

    // tsdb相关配置项，用于上报数据到tsdb中
    private String tsdbUrl = "";
    private String tsdbDbname = "";
    private String tsdbUserPass = "";
    private final Map<String, String> optArgs = new HashMap<>();
    private final ArrayBlockingQueue<String> buffer = new ArrayBlockingQueue<>(100);
    private final ExecutorService es;
    private volatile boolean isStop = false;

    /**
     * 构造函数
     */
    private TsdbWriter() {
        DatabusProps props = DatabusProps.getInstance();
        props.originalsWithPrefix(METRIC_TSDB_OPTARGS_PREFIX).forEach((k, v) -> optArgs.put(k, v.toString()));

        // 初始化需要的资源，从配置文件中获取所需配置项
        tsdbUrl = props.getOrDefault(Consts.METRIC_TSDB_URL, "");
        tsdbDbname = props.getOrDefault(Consts.METRIC_TSDB_DBNAME, "");
        String tsdbUser = props.getOrDefault(Consts.METRIC_TSDB_USER, "");
        String tsdbPass = props.getOrDefault(Consts.METRIC_TSDB_PASS, "");

        if (StringUtils.isNoneBlank(tsdbUser) && StringUtils.isNoneBlank(tsdbPass)) {
            // 将tsdb的账户和密码用 : 符号连接成一个字符串
            String instanceKey = props.getOrDefault(Consts.INSTANCE_KEY, "");
            String rootKey = props.getOrDefault(Consts.ROOT_KEY, "");
            String keyIV = props.getOrDefault(Consts.KEY_IV, "");
            if (StringUtils.isNotBlank(instanceKey)) {
                tsdbPass = KeyGen.decrypt(tsdbPass, rootKey, keyIV, instanceKey, tsdbPass);
            }
            tsdbUserPass = tsdbUser + ":" + tsdbPass;
        }
        // 触发数据刷盘
        es = Executors.newFixedThreadPool(3, new ThreadFactoryBuilder().setNameFormat("tsdb_writer-%d").build());
        es.submit(() -> {
            while (!isStop) {
                try {
                    flush();
                    Thread.sleep(500);
                } catch (InterruptedException ignore) {
                    log.info("got interrupted during sleep: {}", ignore.getMessage());
                    return;
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                isStop = true;
                es.shutdown();
                flush();
            } catch (Exception e) {
                log.warn("exception in shutdown hook for TsdbWriter: {}", e.getMessage());
            }
        }));
    }

    /**
     * 内部类
     */
    private static class Holder {

        private static final TsdbWriter INSTANCE = new TsdbWriter();
    }

    /**
     * 获取TsdbWriter实例
     *
     * @return TsdbWriter实例
     */
    public static final TsdbWriter getInstance() {
        return Holder.INSTANCE;
    }


    /**
     * 上报数据到tsdb集群中
     *
     * @param measurement tsdb的表名
     * @param tagsStr tags内容
     * @param fieldsStr fields内容
     * @param ts 数据时间戳，秒
     */
    public void reportData(String measurement, String tagsStr, String fieldsStr, long ts) {
        String data = String.format("%s,%s %s %s000000000", measurement, tagsStr, fieldsStr, ts);
        reportData(data);
    }


    /**
     * 上报数据到tsdb集群中
     *
     * @param data 目标数据
     */
    public void reportData(String data) {
        if (!buffer.offer(data)) {
            LogUtils.debug(log, "buffer is full, going to flush!");
            es.submit(this::flush);  // 异步处理
            if (!buffer.offer(data)) {  // 再尝试添加一次
                LogUtils.warn(log, "adding failed, data: {}", data);
            }
        }
    }

    /**
     * 将缓存中的数据刷盘到tsdb中
     */
    public void flush() {
        if (!buffer.isEmpty()) {
            // 将数据组装起来，每行一条数据
            StringBuilder data = new StringBuilder();
            String line = buffer.poll();
            while (line != null) {
                data.append(line).append("\n");
                line = buffer.poll();
            }
            // 去掉最后一个换行符
            data.deleteCharAt(data.length() - 1);
            // 将数据提交到tsdb中
            InfluxdbUtils.submitData(tsdbDbname, data.toString(), tsdbUrl, tsdbUserPass, optArgs);
        }
    }

}
