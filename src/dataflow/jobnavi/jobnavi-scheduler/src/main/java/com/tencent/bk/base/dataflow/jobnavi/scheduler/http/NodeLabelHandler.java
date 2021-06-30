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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.HeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.NodeLabelManager;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class NodeLabelHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(NodeLabelHandler.class);

    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        String operate = (String) params.get("operate");
        try {
            if ("add_label".equals(operate)) {
                String labelName = (String) params.get("label_name");
                String description = (String) params.get("description");
                NodeLabelManager.addLabel(labelName, description);
            } else if ("add_host_label".equals(operate)) {
                String labelName = (String) params.get("label_name");
                String host = (String) params.get("host");
                String description = (String) params.get("description");
                NodeLabelManager.addHostLabel(labelName, host, description);
            } else if ("delete_host_label".equals(operate)) {
                String host = (String) params.get("host");
                String labelName = (String) params.get("label_name");
                NodeLabelManager.deleteHostLabel(host, labelName);
            } else if ("delete_all_host_label".equals(operate)) {
                String host = (String) params.get("host");
                NodeLabelManager.deleteAllHostLabel(host);
            } else if ("delete_label".equals(operate)) {
                String labelName = (String) params.get("label_name");
                NodeLabelManager.deleteLabel(labelName);
            } else {
                throw new IllegalArgumentException("operate " + operate + " is not support.");
            }
        } catch (Throwable e) {
            LOGGER.error(e);
            writeData(false, e.getMessage(), response);
        }
        writeData(true, "", response);
    }

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String operate = request.getParameter("operate");
        if ("list_host_label".equals(operate)) {
            Set<String> hosts = HeartBeatManager.getNodeInfos().keySet();
            Map<String, Set<String>> hostLabels = new HashMap<>();
            for (String host : hosts) {
                hostLabels.put(host, NodeLabelManager.getLabels(host));
            }
            writeData(true, "list host label succeeded", HttpReturnCode.DEFAULT,
                    JsonUtils.writeValueAsString(hostLabels), response);
        } else if ("get_host_label".equals(operate)) {
            String host = request.getParameter("host");
            writeData(true, "get label of host:" + host + " succeeded", HttpReturnCode.DEFAULT,
                    JsonUtils.writeValueAsString(NodeLabelManager.getLabels(host)), response);
        } else if ("list_label".equals(operate)) {
            writeData(true, "list label definition succeeded", HttpReturnCode.DEFAULT,
                    JsonUtils.writeValueAsString(NodeLabelManager.getAllLabel()), response);
        } else {
            writeData(false, "operate " + operate + " not support.", HttpReturnCode.ERROR_PARAM_MISSING, response);
        }
    }
}
