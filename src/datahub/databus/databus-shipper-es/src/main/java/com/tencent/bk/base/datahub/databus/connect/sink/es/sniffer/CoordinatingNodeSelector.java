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


package com.tencent.bk.base.datahub.databus.connect.sink.es.sniffer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据节点配置的属性筛选出符合规则的节点
 */
public class CoordinatingNodeSelector implements NodeSelector {

    private static final Logger log = LoggerFactory.getLogger(CoordinatingNodeSelector.class);

    /**
     * 协调节点配置(配置项名称)
     */
    private final String coordinatingAttributeLabel;
    /**
     * 协调节点配置(配置项对应的值)
     */
    private final String coordinatingAttributeValue;

    public CoordinatingNodeSelector(String coordinatingAttributeLabel, String coordinatingAttributeValue) {
        this.coordinatingAttributeLabel = coordinatingAttributeLabel;
        this.coordinatingAttributeValue = coordinatingAttributeValue;
    }

    /**
     * 根据筛选规则从原始的{@link Node}筛选出符合要求的节点
     *
     * @param original 原始节点列表
     * @return 符合要求的节点列表
     */
    @Override
    public void select(Iterable<Node> original) {

        boolean foundOne = false;
        for (Node node : original) {
            Map<String, List<String>> attributesMap = node.getAttributes();
            if (null == attributesMap) {
                continue;
            }

            List<String> attributes = attributesMap.get(coordinatingAttributeLabel);
            if (null != attributes && attributes.contains(coordinatingAttributeValue)) {
                foundOne = true;
                break;
            }
        }

        if (foundOne) {
            Iterator<Node> nodesIt = original.iterator();
            while (nodesIt.hasNext()) {
                Node node = nodesIt.next();
                Map<String, List<String>> attributesMap = node.getAttributes();
                if (null == attributesMap) {
                    continue;
                }

                List<String> attributes = attributesMap.get(coordinatingAttributeLabel);
                if (null == attributes || !attributes.contains(coordinatingAttributeValue)) {
                    nodesIt.remove();
                }
            }
        }
    }
}
