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

import com.google.common.collect.Lists;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import com.tencent.bk.base.datahub.hubmgr.utils.JmxUtils.JmxConnManager;
import java.lang.Thread.State;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JmxCollectorService implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(JmxCollectorService.class);

    // 集群名称
    private String clusterName;

    // 集群类型
    private String clusterType;

    // 集群jmx节点
    private List<String> jmxAddress;

    // 采集配置
    private List<Object> beanConf;

    /**
     * 构造方法
     *
     * @param clusterName 集群名称
     * @param clusterType 集群类型
     * @param jmxAddress jmx地址
     * @param beanConf 配置
     */
    public JmxCollectorService(String clusterName, String clusterType, List<String> jmxAddress, List<Object> beanConf) {
        this.clusterName = clusterName;
        this.clusterType = clusterType;
        this.jmxAddress = jmxAddress;
        this.beanConf = beanConf;
    }

    /**
     * 按照集群采集，任何jmx采集都带上集群名称，ip节点纬度
     */
    public void run() {
        LogUtils.info(log, "start collect jmx metric, clusterType: {}, clusterName:{}", clusterType, clusterName);
        // 验证配置
        if (StringUtils.isEmpty(clusterName) || (beanConf == null || beanConf.size() == 0)
                || (jmxAddress == null || jmxAddress.size() == 0)) {
            LogUtils.warn(log, "warn, jmx conf is error...");
            return;
        }
        // 同一批数据用同一时间戳，避免group by time（1m）出现偏差
        long time = System.currentTimeMillis() / 1000;
        try {
            //遍历 jmx节点
            for (String address : jmxAddress) {
                JmxConnManager.addConn(address.split(":")[0], Integer.parseInt(address.split(":")[1]), null, null);
                MBeanServerConnection conn;
                try {
                    conn = JmxConnManager.getConn(address);
                } catch (Exception e) {
                    LogUtils.warn(log, "current jmx address is error, {}, {}, {}", clusterName, clusterType, address);
                    JmxConnManager.removeConnInfo(address);
                    continue;
                }
                // 指标集合
                List<Map<String, Object>> metrics = new ArrayList<>();
                List<Map<String, Object>> messages = new ArrayList<>();

                // 遍历所有mBean配置
                for (Object conf : beanConf) {
                    Map<String, Object> curMBean = (Map<String, Object>) conf;
                    String bean = (String) curMBean.get("mbean");
                    List<Map<String, String>> attrs = (List<Map<String, String>>) curMBean.get("attr");
                    String output = (String) curMBean.get("output");
                    // objName 作为表名，然后针对节点指标，添加ip等纬度信息
                    // objNames 为空，说明已经指定了objName，直接取相关属性值
                    ObjectName obj = new ObjectName(bean);
                    try {
                        // jvm 线程需要通过jmx，dump 线程详细信息作为监控,不好通过配置来控制，特殊处理
                        if (MgrConsts.THREAD_NAME.equals(bean)) {
                            metrics.add(dumpThreadInfo(address, output));
                            continue;
                        }
                    } catch (Exception e) {
                        LogUtils.warn(log, "dump thread info error, {}, {}, {}", clusterName, clusterType, address);
                        continue;
                    }
                    List<String> objNames = (List<String>) curMBean.get("objname");
                    // 正常jmx配置采集
                    if (objNames == null) {
                        metrics.addAll(collectorMetricsForObjName(conn, attrs, obj, output));
                    } else {
                        otherCollectorMetrics(conn, metrics, curMBean, objNames, address);
                    }
                }
                metrics.forEach(metric -> messages.add(transMessage(address, metric, time)));
                LogUtils.info(log, "prepare to report jmx metric");
                // 上报jmx打点信息
                reportMetrics(messages);
            }
        } catch (Exception e) {
            LogUtils.warn(log, "jmx collector runner is error, {}, {}, {}", clusterName, clusterType, jmxAddress, e);
        }
    }

    /**
     * 其他定制采集指标
     *
     * @param conn 连接
     * @param metrics 指标集合
     * @param curMBean bean
     * @param objNames objNames
     * @throws Exception exception
     */
    private void otherCollectorMetrics(MBeanServerConnection conn, List<Map<String, Object>> metrics,
            Map<String, Object> curMBean, List<String> objNames, String address) throws Exception {
        String bean = (String) curMBean.get("mbean");
        List<Map<String, String>> attrs = (List<Map<String, String>>) curMBean.get("attr");
        String groupLink = (String) curMBean.get("grouplink");
        String output = (String) curMBean.get("output");
        // objName 作为表名，然后针对节点指标，添加ip等纬度信息
        // objNames 为空，说明已经指定了objName，直接取相关属性值
        ObjectName obj = new ObjectName(bean);

        // 如果是包含* ,则取所有的objName
        List<String> allNames = objNames.toString().contains("*") ? null : objNames;
        if (groupLink != null) {
            // * ,需要确定 group 是否存在关联，若存在关联，需要先找出存在关联的group信息，否则直接遍历采集
            // 先查出关联的group
            ObjectName linkObj = new ObjectName(groupLink);
            MBeanServerConnection linkConn = JmxConnManager.getConn(address);
            Set<ObjectInstance> linkMBeanSet = linkConn.queryMBeans(linkObj, null);
            List<String> groupList = new ArrayList<>();
            linkMBeanSet.forEach(objIns -> {
                groupList.add(String.format(bean, objIns.getObjectName().getKeyProperty("name")));
            });
            // 开始构造bean遍历采集
            metrics.addAll(collectorMetricsForLinkObjName(address, attrs, groupList, allNames, output));
        } else {
            // 不为*，则取指定几个objName采集
            metrics.addAll(collectorMetricsForAllObjName(conn, attrs, obj, allNames, output));
        }
    }

    /**
     * 构造打点message
     *
     * @param address jmx地址信息
     * @param metric 指标信息
     * @param time 时间
     */
    private Map<String, Object> transMessage(String address, Map<String, Object> metric, long time) {
        String measurement = (String) metric.remove("output");
        String beanName = (String) metric.remove("mName");
        String beanGroup = (String) metric.remove("mGroup");
        String attrName = (String) metric.remove("name");
        // 添加默认纬度信息
        Map<String, Object> tags = new HashMap<String, Object>() {
            {
                put("clusterName", clusterName);
                put("clusterType", clusterType);
                put("ip", address.split(":")[0]);
                put("mName", beanName);
                put("mGroup", beanGroup);
                if (attrName != null) {
                    put("aName", attrName);
                }
            }
        };
        // 构造打点消息
        Map<String, Object> message = new HashMap<String, Object>() {
            {
                put("data", metric);
                put("tags", tags);
                put("measurement", measurement);
                put("time", time);
            }
        };
        return message;
    }


    /**
     * 获取指标值
     *
     * @param conn bean连接
     * @param obj obj
     * @param attrName 指标属性
     * @param type 字段类型
     * @return 指标值
     * @throws Exception 异常
     */
    private Object getAttr(MBeanServerConnection conn, ObjectName obj, String attrName, String type) throws Exception {
        Object result = null;
        Object attr = conn.getAttribute(obj, attrName);
        switch (type) {
            case MgrConsts.INT:
                result = Integer.valueOf(attr == null ? "0" : attr.toString());
                break;
            case MgrConsts.LONG:
                result = Long.valueOf(attr == null ? "0L" : attr.toString());
                break;
            case MgrConsts.STRING:
                result = attr == null ? "" : attr.toString();
                break;
            case MgrConsts.FLOAT:
                result = Float.valueOf(String.valueOf(attr == null ? 0.0f : attr));
                break;
            case MgrConsts.DOUBLE:
                result = Double.valueOf(String.valueOf(attr == null ? 0.0d : attr));
                break;
            case MgrConsts.MEMORY_USAGE:
                CompositeDataSupport memory = (CompositeDataSupport) attr;
                result = new HashMap<String, Object>() {
                    {
                        put("init", Long.valueOf(String.valueOf(memory.get("init"))));
                        put("committed", Long.valueOf(String.valueOf(memory.get("committed"))));
                        put("max", Long.valueOf(String.valueOf(memory.get("max"))));
                        put("used", Long.valueOf(String.valueOf(memory.get("used"))));
                    }
                };
                break;
            default:
                LogUtils.info(log, "does not support type: {}-{}", attrName, type);
        }
        return result;
    }

    /**
     * 采集单个obj指标
     *
     * @param conn mBean 连接
     * @param attrs 指标属性
     * @param obj obj实例
     * @param output 输出
     * @return 返回指标结果集
     * @throws Exception 异常
     */
    private List<Map<String, Object>> collectorMetricsForObjName(MBeanServerConnection conn,
            List<Map<String, String>> attrs,
            ObjectName obj,
            String output) throws Exception {
        return new ArrayList<Map<String, Object>>() {
            {
                Map<String, Object> metric = new HashMap<>();

                for (Map<String, String> curAttr : attrs) {
                    // 对于特殊类型需要特殊处理，比如jvm内存采集
                    if (MgrConsts.MEMORY_USAGE.equals(curAttr.get("type"))) {
                        metric.put("name", curAttr.get("name"));
                        metric.putAll(
                                (Map<String, Object>) getAttr(conn, obj, curAttr.get("name"), curAttr.get("type")));
                    } else {
                        metric.put(curAttr.get("name"), getAttr(conn, obj, curAttr.get("name"), curAttr.get("type")));
                    }
                }
                // 增加name纬度
                metric.put("mName", obj.getKeyProperty("name"));
                metric.put("output", output);
                metric.put("mGroup", obj.getKeyProperty("group"));
                add(metric);
            }
        };
    }


    /**
     * 采集指标，包含group所有obj
     *
     * @param conn mBean 连接
     * @param attrs 指标属性
     * @param obj obj实例
     * @param objNames objName
     * @return 返回指标结果集
     * @throws Exception 异常
     */
    private List<Map<String, Object>> collectorMetricsForAllObjName(MBeanServerConnection conn,
            List<Map<String, String>> attrs,
            ObjectName obj,
            List<String> objNames,
            String output) throws Exception {
        return new ArrayList<Map<String, Object>>() {
            {
                Set<ObjectInstance> beanSet = conn.queryMBeans(obj, null);
                for (ObjectInstance objIns : beanSet) {
                    String name = objIns.getObjectName().getCanonicalName();
                    if (objNames == null || objNames.contains(objIns.getObjectName().getKeyProperty("name"))) {
                        ObjectName subObj = new ObjectName(name);
                        addAll(collectorMetricsForObjName(conn, attrs, subObj, output));
                    }
                }
            }
        };
    }

    /**
     * 采集指标，包含关联group
     *
     * @param address jmx地址
     * @param attrs 采集属性
     * @param groupList group关联的mbean
     * @param objNames objectName
     * @return 返回指标结果集
     * @throws Exception 异常
     */
    private List<Map<String, Object>> collectorMetricsForLinkObjName(String address,
            List<Map<String, String>> attrs,
            List<String> groupList,
            List<String> objNames,
            String output) throws Exception {
        return new ArrayList<Map<String, Object>>() {
            {
                for (String group : groupList) {
                    ObjectName obj = new ObjectName(group);
                    MBeanServerConnection conn = JmxConnManager.getConn(address);
                    addAll(collectorMetricsForAllObjName(conn, attrs, obj, objNames, output));
                }
            }
        };
    }


    /**
     * dump jvm 线程相信信息
     *
     * @param address jmx地址
     * @param output 输出
     * @return 现场指标信息
     */
    public Map<String, Object> dumpThreadInfo(String address, String output) throws Exception {
        Map<String, Object> metric = new HashMap<String, Object>() {
            {
                put("output", output);
            }
        };
        ThreadMXBean threadBean = JmxConnManager.getThreadMBean(address);
        ThreadInfo[] allThreads = threadBean.dumpAllThreads(false, false);
        metric.put("deamon", threadBean.getDaemonThreadCount());
        metric.put("total", threadBean.getThreadCount());

        HashMap<State, Integer> state = new HashMap<>();
        for (ThreadInfo info : allThreads) {
            State threadState = info.getThreadState();

            Integer vl = state.get(threadState);
            if (vl == null) {
                state.put(threadState, 0);
            } else {
                state.put(threadState, vl + 1);
            }
        }

        metric.put("timed_waiting", state.getOrDefault(State.TIMED_WAITING, 0));
        metric.put("waiting", state.getOrDefault(State.WAITING, 0));
        metric.put("runnable", state.getOrDefault(State.RUNNABLE, 0));
        metric.put("blocked", state.getOrDefault(State.BLOCKED, 0));
        metric.put("terminated", state.getOrDefault(State.TERMINATED, 0));

        return metric;
    }


    /**
     * 构建上报指标参数
     *
     * @param messages 指标消息集
     */
    private void reportMetrics(List<Map<String, Object>> messages) {
        try {
            // 此处分批上报，防止上报400
            List<List<Map<String, Object>>> bucketList = Lists.partition(messages, MgrConsts.REPORT_BATCH_COUNT);
            for (List<Map<String, Object>> curBucket : bucketList) {
                // 分批上报，若上报存在失败，则记录日志，不影响下一次上报
                List<Map<String, Object>> batchMessages = new ArrayList<>();
                Map<String, Object> params = new HashMap<String, Object>() {
                    {
                        put("kafka_topic", MgrConsts.MONITOR_TOPIC);
                        put("tags", Collections.singletonList(MgrConsts.DEFAULT_GEOG_AREA_TAG));
                    }
                };
                curBucket.forEach(map -> {
                    Map<String, Object> data = (Map<String, Object>) map.get("data");
                    data.put("tags", map.get("tags"));

                    Map<String, Object> entry = new HashMap<>();
                    entry.put((String) map.get("measurement"), data);
                    entry.put("database", MgrConsts.CAPACITY_DATABASE);
                    entry.put("time", map.get("time"));
                    batchMessages.add(entry);
                });
                params.put("message", batchMessages);
                CommUtils.reportMetrics(params);
            }
        } catch (Exception e) {
            LogUtils.warn(log, "report metrics happened exception: {}", CommUtils.getStackTrace(e, 1000));
        }
    }
}
