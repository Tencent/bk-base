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

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Logger;

public class YarnUtil {

    private static final Logger logger = Logger.getLogger(YarnUtil.class);

    /**
     * get yarn application
     *
     * @param applicationId
     * @return yarn application report
     * @throws IOException
     * @throws YarnException
     */
    public static ApplicationReport getApplication(String applicationId) throws IOException, YarnException {
        Configuration conf = new Configuration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        EnumSet<YarnApplicationState> set = EnumSet.noneOf(YarnApplicationState.class);
        set.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> runningApplications = yarnClient.getApplications(set);
        for (ApplicationReport runningApplication : runningApplications) {
            //logger.info("current running application: " + runningApplication.getApplicationId().toString());
            if (applicationId.equals(runningApplication.getApplicationId().toString())) {
                return runningApplication;
            }
        }
        yarnClient.stop();
        return null;
    }

    /**
     * get yarn application report
     *
     * @param applicationId
     * @return yarn application report
     * @throws IOException
     * @throws YarnException
     */
    public static ApplicationReport getApplicationInAllState(String applicationId) throws IOException, YarnException {
        Configuration conf = new Configuration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        try {
            ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
            return yarnClient.getApplicationReport(appId);
        } finally {
            yarnClient.stop();
        }
    }

    /**
     * get yarn application by name
     * @param name
     * @return yarn application report
     * @throws IOException
     * @throws YarnException
     */
    public static ApplicationReport getApplicationByName(String name) throws IOException, YarnException {
        logger.info("query application name: " + name);
        Configuration conf = new Configuration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        EnumSet<YarnApplicationState> set = EnumSet.noneOf(YarnApplicationState.class);
        set.add(YarnApplicationState.NEW);
        set.add(YarnApplicationState.NEW_SAVING);
        set.add(YarnApplicationState.SUBMITTED);
        set.add(YarnApplicationState.ACCEPTED);
        set.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> activeApplications = yarnClient.getApplications(set);
        for (ApplicationReport activeApplication : activeApplications) {
            //logger.info("current running application: " + runningApplication.getApplicationId().toString());
            logger.info("application name: " + activeApplication.getName());
            if (name.equals(activeApplication.getName().trim())) {
                logger.info("find application, id is " + activeApplication.getApplicationId().toString());
                return activeApplication;
            }
        }
        yarnClient.stop();
        return null;
    }

    /**
     * 获取同名的另外一个Flink任务
     *
     * @param name 任务名
     * @param applicationId 需要排除的appid
     * @return yarn application report
     * @throws IOException
     * @throws YarnException
     */
    public static ApplicationReport getApplicationByName(String name, String applicationId)
            throws IOException, YarnException {
        logger.info("query application name: " + name);
        Configuration conf = new Configuration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        EnumSet<YarnApplicationState> set = EnumSet.noneOf(YarnApplicationState.class);
        set.add(YarnApplicationState.NEW);
        set.add(YarnApplicationState.NEW_SAVING);
        set.add(YarnApplicationState.SUBMITTED);
        set.add(YarnApplicationState.ACCEPTED);
        set.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> activeApplications = yarnClient.getApplications(set);
        for (ApplicationReport activeApplication : activeApplications) {
            //logger.info("current running application: " + runningApplication.getApplicationId().toString());
            logger.info("application name: " + activeApplication.getName());
            // 运行的appid
            String runningApplicationId = activeApplication.getApplicationId().toString();
            if (name.equals(activeApplication.getName().trim()) && !applicationId.equals(runningApplicationId)) {
                logger.info("find application by name: id is " + activeApplication.getApplicationId().toString());
                logger.info("find application by name: app type is " + activeApplication.getApplicationType());
                // 限定Flink任务
                if (activeApplication.getApplicationType().toLowerCase().contains("flink")) {
                    logger.info("find duplicate flink application, id is " + activeApplication.getApplicationId()
                            .toString());
                    return activeApplication;
                }
            }
        }
        yarnClient.stop();
        return null;
    }

    /**
     * kill yarn application
     *
     * @param applicationId
     * @throws IOException
     * @throws YarnException
     */
    public static void killApplication(String applicationId) throws IOException, YarnException {
        Configuration conf = new Configuration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        yarnClient.killApplication(appId);
        yarnClient.stop();
    }

    /**
     * check yarn queue resource
     *
     * @param queueName
     * @param submitResource
     * @return ture if there's enough resource in queue
     * @throws NaviException
     */
    public static boolean isValidSubmitApplication(String queueName, Long submitResource) throws NaviException {
        logger.info("check resources: " + submitResource);
        return submitResource < getQueueAvailableMB(queueName);
    }

    private static long getQueueAvailableMB(String queueName) throws NaviException {
        try {
            YarnConfiguration conf = new YarnConfiguration();
            String url = getRMWebAppURL(conf);
            String[] queueLevels = queueName.split("\\.");
            StringBuilder queueFilter = new StringBuilder();
            queueFilter.append("q0=root,");
            for (int i = 0; i < queueLevels.length; i++) {
                queueFilter.append("q").append((i + 1)).append("=").append(queueLevels[i]);
                if (i != queueLevels.length - 1) {
                    queueFilter.append(",");
                }
            }
            String jmxUrl = "http://" + url + "/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics," + queueFilter
                    .toString();
            logger.info("jmx url: " + jmxUrl);
            String queueInfo = HttpUtils.get(jmxUrl);
            logger.info("valid queueInfo: " + queueInfo);
            Map<String, Object> beans = JsonUtils.readMap(queueInfo);
            Map<String, Object> metrics = (Map<String, Object>) ((List) beans.get("beans")).get(0);
            return Long.parseLong(metrics.get("AvailableMB").toString());
        } catch (Exception e) {
            logger.error("get queue [" + queueName + "] resource error.", e);
            throw new NaviException(e);
        }
    }

    /**
     * 获取yarn 队列资源情况
     *
     * @return 队列资源信息
     * @throws NaviException 获取资源异常
     */
    public static Map<String, Long> getStreamQueuesResources() throws NaviException {
        try {
            YarnConfiguration conf = new YarnConfiguration();
            String url = getRMWebAppURL(conf);
            String jmxUrl = "http://" + url
                    + "/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics"
                    + ",q0=root,q1=root,q2=dataflow,q3=stream,q4=*,q5=*";
            logger.info("yarn's jmx url: " + jmxUrl);
            String queuesInfo = HttpUtils.get(jmxUrl);
            logger.info("valid queues info is " + queuesInfo);
            Map<String, Object> queuesMapInfo = JsonUtils.readMap(queuesInfo);
            if (!queuesMapInfo.containsKey("beans")) {
                String errorMessage = "get stream queues's info error, and the message is " + queuesMapInfo.toString();
                logger.error(errorMessage);
                throw new NaviException(errorMessage);
            }
            List<Map<String, Object>> metrics = (List<Map<String, Object>>) queuesMapInfo.get("beans");

            Map<String, Long> streamQueuesResources = new HashMap<>();
            for (Map<String, Object> metric : metrics) {
                String queueName = metric.get("tag.Queue").toString();
                if (queueName.startsWith("root.root.")) {
                    queueName = queueName.substring(5);
                }
                streamQueuesResources.put(queueName, Long.parseLong(metric.get("AvailableMB").toString()));
            }
            return streamQueuesResources;
        } catch (Exception e) {
            logger.error("get stream queues's resources error.", e);
            throw new NaviException(e);
        }
    }

    private static String getRMWebAppURL(Configuration conf) throws NaviException {
        Collection<String> rmIds = conf.getStringCollection(YarnConfiguration.RM_HA_IDS);
        for (String rmId : rmIds) {
            String rmAddressConfig = HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, rmId);
            String rmAddress = conf.get(rmAddressConfig);
            logger.info("rmAddress is : " + rmAddress);
            String[] hostAndPort = rmAddress.split(":");
            if (isHostConnectable(hostAndPort[0], Integer.parseInt(hostAndPort[1]))) {
                String suffix = HAUtil.addSuffix(YarnConfiguration.RM_WEBAPP_ADDRESS, rmId);
                InetSocketAddress address = conf.getSocketAddr(suffix, YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS,
                        YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);
                return getResolvedAddress(address);
            }
        }
        throw new NaviException("cannot find active resourcemanager.");
    }

    private static boolean isHostConnectable(String host, int port) {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port));
        } catch (IOException e) {
            logger.warn("connect " + host + ":" + port + " error.");
            return false;
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    private static String getResolvedAddress(InetSocketAddress address) {
        address = NetUtils.getConnectAddress(address);
        StringBuilder sb = new StringBuilder();
        InetAddress resolved = address.getAddress();
        if (resolved == null || resolved.isAnyLocalAddress() || resolved.isLoopbackAddress()) {
            String lh = address.getHostName();
            try {
                lh = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                //Ignore and fallback.
            }
            sb.append(lh);
        } else {
            sb.append(address.getHostName());
        }
        sb.append(":").append(address.getPort());
        return sb.toString();
    }


}
