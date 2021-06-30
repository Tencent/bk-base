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

package com.tencent.bk.base.datahub.iceberg;

import static com.tencent.bk.base.datahub.iceberg.C.COLON;
import static com.tencent.bk.base.datahub.iceberg.C.DATA;
import static com.tencent.bk.base.datahub.iceberg.C.HTTP;
import static com.tencent.bk.base.datahub.iceberg.C.ICEBERG_SNAPSHOT;
import static com.tencent.bk.base.datahub.iceberg.C.OPERATION;
import static com.tencent.bk.base.datahub.iceberg.C.SEQUENCE_NUMBER;
import static com.tencent.bk.base.datahub.iceberg.C.SNAPSHOT_ID;
import static com.tencent.bk.base.datahub.iceberg.C.SUMMARY;
import static com.tencent.bk.base.datahub.iceberg.C.TABLE_NAME;
import static com.tencent.bk.base.datahub.iceberg.C.TIMESTAMP;
import static com.tencent.bk.base.datahub.iceberg.C.TYPE;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotEventCollector implements Listener<CreateSnapshotEvent> {

    private static final Logger log = LoggerFactory.getLogger(SnapshotEventCollector.class);
    private static final ObjectMapper OM = new ObjectMapper();

    private static final ArrayBlockingQueue<CreateSnapshotEvent> QUEUE = new ArrayBlockingQueue<>(500);

    private final ExecutorService es;
    private volatile boolean isStop = false;

    private int port = 80;  // 默认为80端口，HTTP请求
    private String reportUrl;

    private SnapshotEventCollector() {
        // 设定一个线程将数据上报到远端
        es = Executors.newFixedThreadPool(5, new ThreadFactoryBuilder().setNameFormat("event-notify-%d").build());
        es.submit(() -> {
            while (!isStop) {
                if (flush() == 0) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        log.info("got interrupted during sleep: {}", e.getMessage());
                        return;
                    }
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                isStop = true;
                es.shutdown();
                flush();
            } catch (Exception e) {
                log.warn("exception in shutdown hook for SnapshotEventCollector: {}", e.getMessage());
            }
        }));
    }

    public static SnapshotEventCollector getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * 设置通过HTTP上报数据的地址
     *
     * @param dns 数据上报的域名，xxx.xx.xxx或xxx.xx.xxx:8080格式
     * @param path URL的路径
     */
    public void setReportUrl(String dns, String path) {
        synchronized (this) {
            if (StringUtils.isBlank(reportUrl)) {
                String host = dns;
                if (dns.contains(COLON)) {
                    String[] hostPort = StringUtils.split(dns, COLON);
                    host = hostPort[0];
                    try {
                        port = Integer.parseInt(hostPort[1]);
                    } catch (NumberFormatException e) {
                        log.warn("failed to parse port from dns: {}", dns);
                    }
                }

                reportUrl = String.format("%s://%s:%s/%s", HTTP, host, port, path);
                log.info("set up report url: {}", reportUrl);
            }
        }
    }


    /**
     * 通知发生创建快照事件
     *
     * @param et 创建快照事件对象
     */
    public void notify(CreateSnapshotEvent et) {
        if (StringUtils.isBlank(reportUrl)) {
            return;
        }

        try {
            if (!QUEUE.offer(et, 50, TimeUnit.MILLISECONDS)) {
                log.warn("adding event to queue failed. {}", eventToString(et));
                es.submit(this::flush);  // 增加处理线程
            }
        } catch (InterruptedException e) {
            log.warn("got interrupted while adding event {} to queue. {}", eventToString(et), e.getMessage());
        } catch (RejectedExecutionException e) {
            log.warn("submit flush() for execution failed. {}", e.getMessage());
        }
    }

    /**
     * 将队列中缓存的事件上报到远端。
     *
     * @return 本轮上报的事件数量
     */
    private int flush() {
        int count = 0;
        if (QUEUE.size() > 0) {
            CreateSnapshotEvent et;
            while ((et = QUEUE.poll()) != null) {
                try {
                    HttpPost request = new HttpPost(reportUrl);
                    String param = getJsonReportEvent(et);
                    request.addHeader("Content-Type", "application/json");
                    request.addHeader("charset", "utf-8");
                    request.setEntity(new StringEntity(param));

                    HttpClient client = new DefaultHttpClient();
                    HttpResponse response = client.execute(request);
                    StatusLine status = response.getStatusLine();
                    if (status.getStatusCode() != 200) {
                        String res = EntityUtils.toString(response.getEntity());
                        log.warn("got bad response for {} with param {}: {} / {}", reportUrl, param, status, res);
                    }

                    count++;
                } catch (IOException | ParseException e) {
                    log.warn("reporting event {} failed! Exception: {}", eventToString(et), e.getMessage());
                }
            }
        }

        return count;
    }

    /**
     * 将创建快照事件转换为json的字符串
     *
     * @param et 创建快照事件对象
     * @return 字符串，json格式
     * @throws IOException IO异常
     */
    private String getJsonReportEvent(CreateSnapshotEvent et) throws IOException {
        Map<String, Object> m = new HashMap<>();
        m.put(TABLE_NAME, et.tableName());
        m.put(OPERATION, et.operation());
        m.put(SNAPSHOT_ID, et.snapshotId());
        m.put(SEQUENCE_NUMBER, et.sequenceNumber());
        m.put(SUMMARY, et.summary());
        m.put(TIMESTAMP, System.currentTimeMillis());

        Map<String, Object> eventMap = new HashMap<>();
        eventMap.put(TYPE, ICEBERG_SNAPSHOT);
        eventMap.put(DATA, m);

        return OM.writeValueAsString(eventMap);
    }

    /**
     * 将event转换为字符串。
     *
     * @param et 创建快照事件
     * @return 事件的字符串值
     */
    private String eventToString(CreateSnapshotEvent et) {
        return String.format("[%s=%s, %s=%s, %s=%s, %s=%s]", TABLE_NAME, et.tableName(),
                OPERATION, et.operation(), SNAPSHOT_ID, et.snapshotId(), SUMMARY, et.summary());
    }

    // 单例模式
    private static class Holder {

        private static final SnapshotEventCollector INSTANCE = new SnapshotEventCollector();

        static {
            Listeners.register(INSTANCE, CreateSnapshotEvent.class);
        }
    }

}
