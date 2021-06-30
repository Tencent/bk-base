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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class DependencyInfo implements Cloneable {

    private String parentId;
    private DependencyRule rule = DependencyRule.all_finished;
    private DependencyParamType type = DependencyParamType.fixed;
    private String value;
    private String windowOffset;

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public DependencyRule getRule() {
        return rule;
    }

    public void setRule(DependencyRule rule) {
        this.rule = rule;
    }

    public DependencyParamType getType() {
        return type;
    }

    public void setType(DependencyParamType type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getWindowOffset() {
        return windowOffset;
    }

    public void setWindowOffset(String windowOffset) {
        this.windowOffset = windowOffset;
    }

    public AbstractDependencyParam getParam(long baseTimeMills, TimeZone timeZone, Period parentPeriod) {
        return DependencyParamFactory.parseParam(type, value, baseTimeMills, timeZone, parentPeriod);
    }

    public AbstractDependencyParam getParam(long baseTimeMills, TimeZone timeZone) {
        return DependencyParamFactory.parseParam(type, value, baseTimeMills, timeZone);
    }

    /**
     * parse dependency info json string
     *
     * @param jsonMap
     */
    public void parseJson(Map<?, ?> jsonMap) {
        if (jsonMap.get("dependency_rule") != null) {
            rule = DependencyRule.valueOf(jsonMap.get("dependency_rule").toString());
        }

        if (jsonMap.get("param_type") != null) {
            type = DependencyParamType.valueOf(jsonMap.get("param_type").toString());
        }

        parentId = jsonMap.get("parent_id").toString();
        if (jsonMap.get("param_value") != null) {
            value = jsonMap.get("param_value").toString();
        }
        if (jsonMap.get("window_offset") != null) {
            windowOffset = jsonMap.get("window_offset").toString();
        }
    }

    public Map<String, Object> toHTTPJson() {
        Map<String, Object> httpJson = new HashMap<>();
        httpJson.put("parent_id", parentId);
        httpJson.put("dependency_rule", rule.name());
        httpJson.put("param_type", type.name());
        httpJson.put("param_value", value);
        httpJson.put("window_offset", windowOffset);
        return httpJson;
    }

    @Override
    public DependencyInfo clone() {
        try {
            return (DependencyInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e.getMessage());
        }
    }

}
