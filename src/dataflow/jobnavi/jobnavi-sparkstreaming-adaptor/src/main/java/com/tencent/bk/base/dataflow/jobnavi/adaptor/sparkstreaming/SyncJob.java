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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.sparkstreaming;

import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class SyncJob {

    /**
     * sync running spark streaming job on yarn
     *
     * @param task
     * @throws Exception
     */
    public static List<String> syncRunningJob(SparkStreamingSubmitTask task) throws Exception {
        List<String> applicationIds = sync(task);
        task.setSparkApplicationIds(applicationIds);
        return applicationIds;
    }

    /**
     * sync job info
     *
     * @param task
     * @return application ID list
     * @throws Exception
     */
    private static List<String> sync(SparkStreamingSubmitTask task) throws Exception {
        final Logger logger = task.getLogger();
        try {
            // spark rest api https://spark.apache.org/docs/latest/monitoring.html
            String jobURL = task.getWebInterfaceUrl() + "/api/v1/applications?status=running";
            logger.info("sync jobs: " + jobURL);
            String jobInfo = HttpUtils.get(jobURL);
            logger.info("jobinfo: " + jobInfo);
            if (!checkIsJson(jobInfo)) {
                return null;
            }
            List<?> applications = JsonUtils.readList(jobInfo);
            if (applications != null && applications.size() > 0) {
                List<String> applicationIds = new ArrayList<>();
                for (Object obj : applications) {
                    if (obj instanceof Map<?, ?>) {
                        String id = (String) ((Map<?, ?>) obj).get("id");
                        if (id != null) {
                            applicationIds.add(id);
                        }
                    }
                }
                logger.info("applicationIds: " + Arrays.toString(applicationIds.toArray()));
                return applicationIds;
            }
        } catch (Throwable e) {
            logger.error("sync job error.", e);
            throw new Exception(e);
        }
        return null;
    }

    private static boolean checkIsJson(String jobInfo) {
        if (StringUtils.isNotBlank(jobInfo)) {
            try {
                JsonUtils.readList(jobInfo);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }
}
