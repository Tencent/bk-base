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

import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;

import java.util.Map;
import java.util.Objects;

public abstract class AbstractTaskEvent implements TaskEvent {

    private EventContext context;
    private TaskStatus changeStatus;
    private String eventName;

    /**
     * parse task event json
     *
     * @param params json map
     * @return ture if parse OK
     */
    @Override
    public boolean parseJson(Map<?, ?> params) {
        eventName = (String) params.get("eventName");
        if (params.get("changeStatus") != null) {
            changeStatus = TaskStatus.valueOf((String) params.get("changeStatus"));
        }
        if (params.get("context") != null && params.get("context") instanceof Map<?, ?>) {
            context = new EventContext();
            return context.parseJson((Map<?, ?>) params.get("context"));
        } else {
            return false;
        }
    }

    @Override
    public String toJson() {
        return JsonUtils.writeValueAsString(this);
    }

    /**
     * load from another event
     *
     * @param event task event
     */
    public void loadFromEvent(TaskEvent event) {
        context = event.getContext() != null ? event.getContext().clone() : null;
        changeStatus = event.getChangeStatus();
        eventName = event.getEventName();
    }

    @Override
    public EventContext getContext() {
        return context;
    }

    public void setContext(EventContext context) {
        this.context = context;
    }

    @Override
    public TaskStatus getChangeStatus() {
        return changeStatus;
    }

    @Override
    public void setChangeStatus(TaskStatus changeStatus) {
        this.changeStatus = changeStatus;
    }

    @Override
    public String getEventName() {
        return eventName;
    }

    @Override
    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractTaskEvent that = (AbstractTaskEvent) o;
        return context.equals(that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context);
    }

    @Override
    public int compareTo(TaskEvent o) {
        double thisRank = this.getContext().getExecuteInfo().getRank();
        double thatRank = o.getContext().getExecuteInfo().getRank();
        //event with greater rank have priority
        if (thisRank > thatRank) {
            return -1;
        } else if (thisRank < thatRank) {
            return 1;
        } else { //thisRank == thatRank
            if (this.getContext() != null && this.getContext().getExecuteInfo() != null
                    && o.getContext() != null && o.getContext().getExecuteInfo() != null) {
                //event with smaller execute ID  have priority
                Long thisExecuteId = this.getContext().getExecuteInfo().getId();
                Long thatExecuteId = o.getContext().getExecuteInfo().getId();
                return thisExecuteId.compareTo(thatExecuteId);
            }
        }
        return 0;
    }

    @Override
    public AbstractTaskEvent clone() {
        try {
            AbstractTaskEvent event = (AbstractTaskEvent) super.clone();
            event.setContext(this.context.clone());
            return event;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e.getMessage());
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
