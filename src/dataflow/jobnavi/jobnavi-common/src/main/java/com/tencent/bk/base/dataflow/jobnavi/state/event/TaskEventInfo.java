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

package com.tencent.bk.base.dataflow.jobnavi.state.event;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;

import java.util.Map;
import java.util.Objects;

public class TaskEventInfo implements Comparable<TaskEventInfo> {

    private Long id;
    private TaskEvent taskEvent;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public TaskEvent getTaskEvent() {
        return taskEvent;
    }

    public void setTaskEvent(TaskEvent taskEvent) {
        this.taskEvent = taskEvent;
    }

    /**
     * parse task event info from json string
     *
     * @param map
     * @throws NaviException
     */
    public void parseJson(Map<String, Object> map) throws NaviException {
        if (map.get("id") != null) {
            id = Long.parseLong(map.get("id").toString());
        }
        if (map.get("taskEvent") != null && map.get("taskEvent") instanceof Map<?, ?>) {
            Map<?, ?> taskEventMap = (Map<?, ?>) map.get("taskEvent");
            taskEvent = TaskEventFactory.generateTaskEvent(taskEventMap.get("eventName").toString());
            taskEvent.parseJson(taskEventMap);
        }
    }

    public String toJson() {
        return JsonUtils.writeValueAsString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskEventInfo that = (TaskEventInfo) o;
        return Objects.equals(id, that.id) && Objects.equals(taskEvent, that.taskEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, taskEvent);
    }

    @Override
    public int compareTo(TaskEventInfo o) {
        if (taskEvent.compareTo(o.taskEvent) != 0) {
            return taskEvent.compareTo(o.taskEvent);
        } else {
            return this.id.compareTo(o.id);
        }
    }
}
