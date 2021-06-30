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

package com.tencent.bk.base.dataflow.jobnavi.state;

import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;

import java.util.HashMap;
import java.util.Map;

public class TaskType implements Cloneable {

    public static final String TASK_TYPE_DEFAULT = "default";

    private String name;
    private String tag;
    private String main;
    private String description;
    private String env;
    private String sysEnv;
    private Language language;
    private TaskMode taskMode;
    private boolean recoverable;
    private String createdBy;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getMain() {
        return main;
    }

    public void setMain(String main) {
        this.main = main;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public String getSysEnv() {
        return sysEnv;
    }

    public void setSysEnv(String sysEnv) {
        this.sysEnv = sysEnv;
    }

    public Language getLanguage() {
        return language;
    }

    public void setLanguage(Language language) {
        this.language = language;
    }

    public TaskMode getTaskMode() {
        return taskMode;
    }

    public void setTaskMode(TaskMode taskMode) {
        this.taskMode = taskMode;
    }

    public boolean isRecoverable() {
        return recoverable;
    }

    public void setRecoverable(boolean recoverable) {
        this.recoverable = recoverable;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    /**
     * parse task type from json string
     *
     * @param json
     */
    public boolean parseJson(Map<?, ?> json) {
        if (json.get("name") != null) {
            name = (String) json.get("name");
        }

        if (json.get("tag") != null) {
            tag = (String) json.get("tag");
        }

        if (json.get("main") != null) {
            main = (String) json.get("main");
        }

        if (json.get("description") != null) {
            description = (String) json.get("description");
        }

        if (json.get("env") != null) {
            env = (String) json.get("env");
        }

        if (json.get("sysEnv") != null) {
            sysEnv = (String) json.get("sysEnv");
        }

        if (json.get("language") != null) {
            language = Language.valueOf((String) json.get("language"));
        }

        if (json.get("taskMode") != null) {
            taskMode = TaskMode.valueOf((String) json.get("taskMode"));
        }
        recoverable = json.get("recoverable") != null && (Boolean) json.get("recoverable");
        return name != null && tag != null && main != null && language != null && taskMode != null;
    }

    public String toJson() {
        return JsonUtils.writeValueAsString(this);
    }

    public Map<String, Object> toHttpJson() {
        Map<String, Object> httpJson = new HashMap<>();
        httpJson.put("name", name);
        httpJson.put("tag", tag);
        httpJson.put("main", main);
        httpJson.put("description", description);
        httpJson.put("env", env);
        httpJson.put("sysEnv", sysEnv);
        httpJson.put("language", language.name());
        httpJson.put("taskMode", taskMode.name());
        httpJson.put("recoverable", recoverable);
        return httpJson;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
