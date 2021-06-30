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

package com.tencent.bk.base.dataflow.jobnavi.yarnservice;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.log4j.Logger;

public class YarnUtil {
    private static final Logger logger = Logger.getLogger(YarnUtil.class);
    private static final String APACHE_FLINK = "Apache Flink";

    public static String getYarnState() {
        Configuration conf = new Configuration();
        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        String state = RMHAUtils.findActiveRMHAId(yarnConf);
        return state;
    }

    /**
     * 获取全部正在运行的apps
     *
     * @return 正在运行的apps
     * @throws IOException
     * @throws YarnException
     */
    public static List<AppEntity> getRunningApps() throws IOException, YarnException {
        List<ApplicationReport> runningApplications = Lists.newArrayList();
        Configuration conf = new Configuration();
        try (YarnClient yarnClient = YarnClient.createYarnClient()) {
            yarnClient.init(conf);
            yarnClient.start();
            Set<String> appTypes = Sets.newHashSet();
            appTypes.add(APACHE_FLINK);
            EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
            appStates.add(YarnApplicationState.RUNNING);
            runningApplications = yarnClient.getApplications(appTypes, appStates);
        } catch (Exception e) {
            logger.error(e);
        }
        List<AppEntity> appEntities = runningApplications.stream()
                .map(app -> new AppEntity(app.getApplicationId().toString(), app.getName(), app.getQueue()))
                .collect(Collectors.toList());
        return appEntities;
    }

}
