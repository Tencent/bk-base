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

package com.tencent.bk.base.datahub.databus.pipe;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public abstract class Node {

    protected String type = "";
    protected String label = "";

    public Map<String, Object> nodeConf;
    public Map<String, Object> nextNodeConf;

    protected Node next;

    public abstract boolean validateNext();

    public abstract Object execute(Context ctx, Object o);

    /**
     * 根据节点配置信息获取节点的label
     */
    protected void generateNodeLabel() {
        if (nodeConf != null && nodeConf.containsKey(EtlConsts.LABEL) && nodeConf.get(EtlConsts.LABEL) != null) {
            label = nodeConf.get(EtlConsts.LABEL).toString();
        }

        if (StringUtils.isBlank(label)) {
            // 自动生成label的值
            label = AutoNodeLabel.getAutoLabel();
        }
    }
}
