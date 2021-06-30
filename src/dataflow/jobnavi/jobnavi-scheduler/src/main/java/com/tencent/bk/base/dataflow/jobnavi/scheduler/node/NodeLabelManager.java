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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.node;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.node.NodeLabel;
import com.tencent.bk.base.dataflow.jobnavi.node.NodeLabelHost;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.StringUtils;

public class NodeLabelManager {

    public static final String RESERVED_DEFAULT_LABEL = "default";

    private static final Map<String, Set<String>> hostLabels = new ConcurrentHashMap<>();
    private static final Map<String, String> labels = new ConcurrentHashMap<>();

    /**
     * init node label manager
     *
     * @throws NaviException
     */
    public static void init() throws NaviException {
        List<NodeLabel> nodeLabels = MetaDataManager.getJobDao().listNodeLabel();
        for (NodeLabel nodeLabel : nodeLabels) {
            labels.put(nodeLabel.getName(), nodeLabel.getDescription());
        }

        List<NodeLabelHost> nodeLabelHosts = MetaDataManager.getJobDao().listNodeLabelHost();
        for (NodeLabelHost nodeLabelHost : nodeLabelHosts) {
            bindHostLabel(nodeLabelHost.getLabelName(), nodeLabelHost.getHost());
        }
    }

    /**
     * add node label definition
     *
     * @param labelName
     * @param description
     * @throws NaviException
     */
    public static void addLabel(String labelName, String description) throws NaviException {
        if (labels.containsKey(labelName)) {
            throw new NaviException("labelName [" + labelName + "] has already existed.");
        }
        NodeLabel label = new NodeLabel();
        label.setName(labelName);
        label.setDescription(description);
        MetaDataManager.getJobDao().addNodeLabel(label);
        labels.put(labelName, description);
    }

    /**
     * bind host label
     *
     * @param labelName
     * @param host
     * @param description
     * @throws NaviException
     */
    public static synchronized void addHostLabel(String labelName, String host, String description)
            throws NaviException {
        NodeLabelHost nodeLabelHost = new NodeLabelHost();
        nodeLabelHost.setLabelName(labelName);
        nodeLabelHost.setHost(host);
        nodeLabelHost.setDescription(description);
        MetaDataManager.getJobDao().addHostNodeLabel(nodeLabelHost);
        bindHostLabel(labelName, host);
    }

    /**
     * delete host label
     * @param host
     * @param labelName
     */
    public static void deleteHostLabel(String host, String labelName) throws NaviException {
        MetaDataManager.getJobDao().deleteHostLabel(host, labelName);
        if (hostLabels.containsKey(labelName)) {
            hostLabels.get(labelName).remove(host);
        }
    }

    /**
     * delete all host label
     *
     * @param host
     */
    public static void deleteAllHostLabel(String host) throws NaviException {
        MetaDataManager.getJobDao().deleteAllHostLabel(host);
        for (Map.Entry<String, Set<String>> entry : hostLabels.entrySet()) {
            entry.getValue().remove(host);
        }
    }

    /**
     * delete node label definition
     *
     * @param label
     */
    public static void deleteLabel(String label) throws NaviException {
        MetaDataManager.getJobDao().deleteNodeLabel(label);
        hostLabels.remove(label);
        labels.remove(label);
    }

    /**
     * bind host label
     *
     * @param labelName
     * @param host
     * @throws NaviException
     */
    public static synchronized void bindHostLabel(String labelName, String host) throws NaviException {
        bindHostLabel(labelName, host, false);
    }

    /**
     * bind host label
     *
     * @param labelName
     * @param host
     * @param force add label if not exists
     * @throws NaviException
     */
    public static synchronized void bindHostLabel(String labelName, String host, boolean force) throws NaviException {
        if (!labels.containsKey(labelName)) {
            if (!force) {
                throw new NaviException("Cannot find labelName [" + labelName + "].");
            } else {
                labels.put(labelName, labelName);
            }
        }
        Set<String> hosts = hostLabels.get(labelName);
        if (hosts == null) {
            hosts = new HashSet<>();
            hostLabels.put(labelName, hosts);
        }
        hosts.add(host);
    }

    public static synchronized void unbindHostLabel(String labelName, String host) {
        Set<String> hosts = hostLabels.get(labelName);
        if (hosts != null) {
            hosts.remove(host);
        }
    }

    public static Set<String> getLabels(String host) {
        Set<String> labels = new HashSet<>();
        for (Map.Entry<String, Set<String>> hostLabel : hostLabels.entrySet()) {
            if (hostLabel.getValue().contains(host)) {
                labels.add(hostLabel.getKey());
            }
        }
        return labels;
    }

    public static synchronized Set<String> getHosts(String labelName) {
        if (StringUtils.isEmpty(labelName)) {
            return null;
        }
        return hostLabels.get(labelName);
    }

    public static boolean hostHasLabel(String host) {
        for (Map.Entry<String, Set<String>> hostLabel : hostLabels.entrySet()) {
            if (hostLabel.getValue().contains(host)) {
                return true;
            }
        }
        return false;
    }

    public static Map<String, Set<String>> getAllHostLabel() {
        return hostLabels;
    }

    public static Map<String, String> getAllLabel() {
        return labels;
    }
}
