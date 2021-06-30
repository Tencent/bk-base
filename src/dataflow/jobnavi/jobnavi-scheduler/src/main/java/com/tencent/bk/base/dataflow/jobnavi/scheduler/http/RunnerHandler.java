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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.http;

import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerStatus;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.NodeLabelManager;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class RunnerHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(RunnerHandler.class);


    @Override
    public void doGet(Request request, Response response) throws Exception {
        String operate = request.getParameter("operate");
        switch (operate) {
            case "get_current_runners": {
                writeData(true, "get runner info success.", HttpReturnCode.DEFAULT,
                        JsonUtils.writeValueAsString(HeartBeatManager.getNodeInfos()), response);
            }
            break;
            case "stop_runner": {
                String host = request.getParameter("host");
                if (StringUtils.isEmpty(host)) {
                    writeData(false, "param [host] require.", HttpReturnCode.ERROR_PARAM_MISSING, response);
                    return;
                }
                HeartBeatManager.removeNode(host);
                LOGGER.info("remove runner [" + host + "] success.");
                writeData(true, "remove runner [" + host + "] success.", response);
            }
            break;
            case "get_available_runners": {
                Collection<RunnerInfo> runnerInfos = HeartBeatManager.getNodeInfos().values();
                List<String> hosts = new ArrayList<>();
                for (RunnerInfo runnerInfo : runnerInfos) {
                    if (runnerInfo.getStatus() == RunnerStatus.running) {
                        String host = runnerInfo.getRunnerId();
                        hosts.add(host);
                    }
                }
                writeData(true, "get available runners success.", HttpReturnCode.DEFAULT,
                        JsonUtils.writeValueAsString(hosts), response);
            }
            break;
            case "get_runner_digests": {
                Map<String, Object> digests = new TreeMap<>();
                Map<String, Set<String>> hostLabelMap = NodeLabelManager.getAllHostLabel();
                for (Map.Entry<String, RunnerInfo> entry : HeartBeatManager.getNodeInfos().entrySet()) {
                    RunnerInfo runnerInfo;
                    if (hostLabelMap.containsKey(entry.getKey())) {
                        runnerInfo = new RunnerInfo();
                        runnerInfo.parseJson(JsonUtils.readMap(entry.getValue().toJson())); //clone Runner info
                        runnerInfo.getLabelSet().addAll(hostLabelMap.get(entry.getKey()));
                    } else {
                        runnerInfo = entry.getValue();
                    }
                    digests.put(entry.getKey(), runnerInfo.digest());
                }
                writeData(true, "get runner info success.", HttpReturnCode.DEFAULT,
                        JsonUtils.writeValueAsString(digests), response);
            }
            break;
            default:
                writeData(false, "operate " + operate + " not support.", HttpReturnCode.ERROR_PARAM_MISSING, response);
        }
    }

}
