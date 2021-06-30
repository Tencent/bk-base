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

import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class SyncJob {

    /**
     * sync running flink job
     *
     * @param task
     * @throws Exception
     */
    public static Map<String, String> syncRunningJob(FlinkSubmitTask task) throws Exception {
        String[] filters = {"FAILED", "CANCELED", "FINISHED"};
        Map<String, String> nameAndIdMaps = sync(task, filters);
        task.setJobNameAndIdMaps(nameAndIdMaps);
        return nameAndIdMaps;
    }


    /**
     * sync flink job
     *
     * @param task
     * @return job names and ID
     * @throws Exception
     */
    public static Map<String, String> sync(FlinkSubmitTask task) throws Exception {
        return sync(task, null);
    }

    /**
     * sync job info
     *
     * @param task
     * @param filters filter by status. if null, return all jobs
     * @return job names and IDs
     * @throws Exception
     */
    private static Map<String, String> sync(FlinkSubmitTask task, String[] filters) throws Exception {
        final Logger logger = task.getLogger();
        try {
            String jobURL = task.getWebInterfaceUrl() + "/jobs";
            logger.info("sync jobs: " + jobURL);
            String jobInfo = HttpUtils.get(jobURL);
            logger.info("jobinfo: " + jobInfo);
            Map<String, Object> json = JsonUtils.readMap(jobInfo);
            if (json.size() > 0) {
                List<Map<String, String>> jobInfos = (List<Map<String, String>>) json.get("jobs");
                Map<String, String> nameAndIdMaps = new HashMap<>();
                for (Map<String, String> job : jobInfos) {
                    if (!containFilterStatus(job.get("status").toUpperCase(), filters)) {
                        String runningJobId = job.get("id");
                        String jobDetailURL = jobURL + "/" + runningJobId;
                        logger.info("request job detail from url: " + jobDetailURL);
                        String jobDetailInfo = HttpUtils.get(jobDetailURL);
                        Map<String, Object> jobDetail = JsonUtils.readMap(jobDetailInfo);
                        String jobName = jobDetail.get("name").toString();
                        nameAndIdMaps.put(jobName, runningJobId);
                    }
                }
                logger.info("nameAndIdMaps: " + nameAndIdMaps);
                return nameAndIdMaps;
            }
        } catch (Throwable e) {
            logger.error("sync job error.", e);
            throw new Exception(e);
        }
        return null;
    }

    private static boolean containFilterStatus(String status, String[] filters) {
        if (filters != null) {
            for (String filter : filters) {
                if (status.equals(filter)) {
                    return true;
                }
            }
        }
        return false;
    }
}
