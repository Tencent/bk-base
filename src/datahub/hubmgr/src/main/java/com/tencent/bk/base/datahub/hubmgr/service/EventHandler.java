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

package com.tencent.bk.base.datahub.hubmgr.service;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.bean.EventBean;
import com.tencent.bk.base.datahub.databus.commons.errors.BadConfException;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.NamedThreadFactory;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.MgrNotifyUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.TsdbWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventHandler extends OpKafkaDataHandler {

    private static final Logger log = LoggerFactory.getLogger(EventHandler.class);

    private Map<String, Set<EventBean>> failureEvents = new ConcurrentHashMap<>();
    private Map<String, Integer> tpPauseAlarms = new ConcurrentHashMap<>();
    private Map<String, Long> tpPauseEvents = new ConcurrentHashMap<>();
    private Map<String, Long> tpResumeEvents = new ConcurrentHashMap<>();

    private Integer accessTimes = DatabusProps.getInstance().getOrDefault(MgrConsts.TP_EVENT_ACCESS_TIMES,
            MgrConsts.TP_EVENT_ACCESS_TIMES_DEFAULT);

    /**
     * 构造函数，给反射机制使用
     */
    public EventHandler() {
        // 触发定期数据刷盘
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("recover_task"));
        exec.scheduleAtFixedRate(() -> {
            // 定时触发任务恢复机制
            triggerRecover();
            // 检查暂停分区，触发告警。
            checkTpPause();
        }, 1, 3, TimeUnit.MINUTES);
    }

    /**
     * 获取服务名称
     *
     * @return 服务名称
     */
    public String getServiceName() {
        return MgrConsts.EVENT_HANDLER;
    }

    /**
     * 获取日志对象
     *
     * @return 日志对象
     */
    public Logger getLog() {
        return log;
    }

    /**
     * 获取需要消费的topic列表
     *
     * @return topic列表
     */
    public String getTopic() {
        String topic = DatabusProps.getInstance().getOrDefault(MgrConsts.DATABUSMGR_EVENT_TOPIC,
                MgrConsts.DATABUSMGR_EVENT_TOPIC_DEFAULT);
        if (StringUtils.isBlank(topic)) {
            LogUtils.error(MgrConsts.ERRCODE_BAD_CONFIG, log, "event config is empty, unable to start service!");
            throw new BadConfException("databusmgr.event.topic config is empty, unable to start service!");
        }

        return topic;
    }

    /**
     * 处理kafka数据
     *
     * @param records kafka中的消息
     */
    public void processData(ConsumerRecords<String, String> records) {
        String tag = DatabusProps.getInstance().getOrDefault(MgrConsts.ALARM_TAG, "");
        for (ConsumerRecord<String, String> record : records) {
            // 解析kafka的key/value，处理数据
            if (StringUtils.isNotBlank(record.key()) && StringUtils.isNotBlank(record.value())) {
                try {
                    String[] keyArr = StringUtils.split(record.key(), '|');
                    EventBean bean = JsonUtils.parseBean(record.value(), EventBean.class);
                    String fieldsStr = CommUtils.tsdbFields(bean.getMessage(), bean.getUid());
                    TsdbWriter.getInstance().reportData(MgrConsts.DATABUSMGR_EVENT_TOPIC_DEFAULT,
                            String.format("cluster=%s,ip=%s,event=%s", keyArr[0], keyArr[1], bean.getType()),
                            fieldsStr, bean.getTime());

                    // 针对不同类型的事件，触发相应的动作
                    switch (bean.getType()) {
                        case MgrConsts.TASK_START_FAIL:
                        case MgrConsts.TASK_RUN_FAIL:
                        case MgrConsts.CONNECTOR_START_FAIL:
                            LogUtils.info(log, "processing databus start/run failure event: {}", record.value());
                            putEventToMap(keyArr[0], bean);
                            break;

                        case MgrConsts.TASK_STOP_FAIL:
                        case MgrConsts.CONNECTOR_STOP_FAIL:
                            LogUtils.info(log, "processing databus stop failure event: {}", record.value());
                            // 发送通知给相关人员
                            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(),
                                    DatabusProps.getInstance().getOrDefault(MgrConsts.DATABUS_ADMIN, ""),
                                    tag + " 停止失败 " + record.value());
                            break;
                        case MgrConsts.CACHE_SDK_FAIL:
                            LogUtils.info(log, "client cache sdk failure event: {}", record.value());
                            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(),
                                    DatabusProps.getInstance().getOrDefault(MgrConsts.STOREKIT_ADMIN, ""),
                                            "CacheSdk查询失败 " + record.value());
                            break;
                        case MgrConsts.BATCH_IMPORT_EMPTY_DIR:
                        case MgrConsts.BATCH_IMPORT_BAD_DATA_FILE:
                        case MgrConsts.BATCH_IMPORT_TOO_MANY_RECORDS:
                            LogUtils.info(log, "batch import databus event {}", record.value());
                            // 发送通知给相关人员
                            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(),
                                    DatabusProps.getInstance().getOrDefault(MgrConsts.BATCH_ADMIN, ""),
                                    tag + " 导入数据失败 " + record.value());
                            break;
                        case MgrConsts.ICEBERG_ACQUIRE_LOCK_TIMEOUT:
                            LogUtils.info(log, "iceberg acquire lock timeout databus event {}", record.value());
                            // 发送通知给相关人员
                            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(),
                                    DatabusProps.getInstance().getOrDefault(MgrConsts.STOREKIT_ADMIN, ""),
                                    String.format("%s %s %s", tag, "iceberg提交数据时，获取锁超时", record.value()));
                            break;
                        case MgrConsts.TASK_TP_PAUSE:
                            LogUtils.info(log, "processing databus tp pause event: {}", record.value());
                            String[] tps = bean.getMessage().split(",");
                            for (String tp : tps) {
                                tpPauseEvents.put(key(bean.getUid(), tp), bean.getTime() * 1000);
                            }
                            break;

                        case MgrConsts.TASK_TP_RESUME:
                            LogUtils.info(log, "processing databus tp resume event: {}", record.value());
                            String key = key(bean.getUid(), bean.getMessage());
                            if (tpPauseEvents.get(key) != null) {
                                // 已经恢复的tp, 直接删除
                                tpPauseEvents.remove(key(bean.getUid(), bean.getMessage()));
                            } else {
                                // 可能恢复事件先到，这里保存下来，便于后续告警发送
                                tpResumeEvents.put(key(bean.getUid(), bean.getMessage()), bean.getTime() * 1000);
                            }
                            break;
                        default:
                            LogUtils.info(log, "other databus event to ignore: {}", record.value());
                    }
                } catch (Exception e) {
                    LogUtils.warn(log, String.format("failed to parse kafka message: %s", record), e);
                }
            } else {
                LogUtils.warn(log, "bad record for databus event: {}", record);
            }
        }
    }


    /**
     * 将事件放入缓存中，定时批量处理
     *
     * @param cluster 集群名称
     * @param bean event对象
     */
    private void putEventToMap(String cluster, EventBean bean) {
        if (!failureEvents.containsKey(cluster)) {
            failureEvents.put(cluster, new HashSet<>());
        }
        failureEvents.get(cluster).add(bean);
    }

    /**
     * 调用接口重启失败的总线任务，使集群内失败的任务都重启
     */
    private void triggerRecover() {
        DatabusProps props = DatabusProps.getInstance();
        // 执行实际的检查集群逻辑
        String apiDns = CommUtils.getDns(MgrConsts.API_DATAHUB_DNS);
        String path = props.getOrDefault(MgrConsts.API_RESTART_CONNECTOR, MgrConsts.API_RESTART_CONNECTOR_DEFAULT);
        String receivers = props.getOrDefault(MgrConsts.DATABUS_ADMIN, "");
        String tag = props.getOrDefault(MgrConsts.ALARM_TAG, "");

        failureEvents.forEach((k, v) -> {
            EventBean eventBean = v.iterator().next();
            String failedTask = eventBean.getUid();
            // 发送通知
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(CommUtils.getDatetime()).append("][").append(tag).append("]近3分钟").append(k);
            sb.append("失败任务列表: ").append(failedTask);
            LogUtils.info(log, sb.toString());
            // 触发任务重启
            try {
                String restUrl = String.format("http://%s%s", apiDns,
                        path.replace("CLUSTER", k).replace("CONNECTOR", failedTask));
                ApiResult result = HttpUtils.getApiResult(restUrl);
                if (result.isResult()) {
                    LogUtils.info(log, "finish databus cluster status check. {}",
                            JsonUtils.toJsonWithoutException(result.getData()));
                } else {
                    if ((boolean) result.getErrors()) {
                        String message = "[" + CommUtils.getDatetime() + "][" + tag + "]重启任务失败：\n"
                                + "集群：" + k
                                + "\n任务：" + failedTask
                                + "\n错误：任务丢失！";
                        MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers, message);
                        return;
                    }
                    LogUtils.warn(log, "failed to check databus cluster status, bad response! {}",
                            JsonUtils.toJsonWithoutException(result));
                }
                sb.append("\n重启任务结果: ").append(JsonUtils.toJsonWithoutException(result));
                sb.append("\n任务失败原因: ").append(eventBean.getMessage());
            } catch (Exception e) {
                // ignore
                LogUtils.warn(log, String.format("failed to trigger recover for %s in %s", failedTask, k), e);
                sb.append("\n重启任务失败: ").append(e.getMessage());
            }
            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers, sb.toString());
        });
        // 清空缓存数据
        failureEvents.clear();
    }


    /**
     * 检查未恢复的任务tp
     */
    private void checkTpPause() {
        // 对于连续告警多次的事件，直接删除
        LogUtils.info(log, "current pause partition events {}, resume partition events {}, pause alarms {}",
                tpPauseEvents, tpResumeEvents, tpPauseAlarms);

        // 如果暂停事件为空，则清掉一些脏事件
        if (tpPauseEvents.size() == 0) {
            tpResumeEvents.clear();
            tpPauseAlarms.clear();
            return;
        }

        String receivers = DatabusProps.getInstance().getOrDefault(MgrConsts.STOREKIT_ADMIN, "");
        String pauseMessageFormat = "任务名称: %s %n告警: 存在分区暂停消费 %n暂停分区: %s %n事件时间: %s";
        String resumeMessageFormat = "任务名称: %s %n恢复: 恢复消费分区 %n恢复分区: %s %n事件时间: %s";
        Iterator<Entry<String, Long>> it = tpPauseEvents.entrySet().iterator();
        while (it.hasNext()) {
            // 对于已经暂停的分区，需要告警出来，如果任务已经停止，仍然可能会有告警
            Map.Entry<String, Long> entry = it.next();
            String[] keyArr = entry.getKey().split("#");
            Long pTime = entry.getValue();
            Integer alarmCount = tpPauseAlarms.get(entry.getKey());
            Long rTime = tpResumeEvents.get(entry.getKey());
            //触发告警，并且指明该段时间内，恢复事件和暂停事件时间，便于接收人判断是否为误告
            //初始化告警次数
            if (alarmCount == null) {
                tpPauseAlarms.put(entry.getKey(), accessTimes);
                alarmCount = accessTimes;
            }

            if (alarmCount <= 0) {
                it.remove();
                tpResumeEvents.remove(entry.getKey());
                tpPauseAlarms.remove(entry.getKey());
            } else {
                MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                        String.format(pauseMessageFormat, keyArr[0], keyArr[1],
                        CommUtils.getDatetime(pTime)));
                // 如果存在恢复事件
                if (rTime != null) {
                    LogUtils.info(log, "{}: resume event alarm.", entry.getKey());
                    MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                            String.format(resumeMessageFormat, keyArr[0], keyArr[1],
                            CommUtils.getDatetime(rTime)));
                }

                tpPauseAlarms.put(entry.getKey(), alarmCount - 1);
            }
        }
    }

    /**
     * 生成事件集合key
     */
    private String key(String name, String topicPartition) {
        return String.format("%s#%s", name, topicPartition);
    }
}
