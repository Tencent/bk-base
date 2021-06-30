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

package com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class StateLogicalNode extends AbstractLogicalNode {

    private static final Logger logger = Logger.getLogger(StateLogicalNode.class);
    private String stateType;
    private Map<String, List<String>> stateContent;

    public Map<String, List<String>> getStateContent() {
        return stateContent;
    }

    public void setStateContent(Map<String, List<String>> stateContent) {
        this.stateContent = stateContent;
    }

    public String getStateType() {
        return stateType;
    }

    public void setStateType(String stateType) {
        this.stateType = stateType;
    }

    @Override
    public void parseJson(String id, Map<String, Object> json) throws NaviException {

        super.parseJson(id, json);
        if (json.get("state_type") == null) {
            String message = "state type can't be null";
            logger.error(message);
            throw new NaviException(message);
        }
        if (json.get("state_content") == null) {
            String message = "state content can't be null";
            logger.error(message);
            throw new NaviException(message);
        }
        this.stateType = (String) json.get("state_type");
        this.stateContent = (Map<String, List<String>>) json.get("state_content");
    }
}
