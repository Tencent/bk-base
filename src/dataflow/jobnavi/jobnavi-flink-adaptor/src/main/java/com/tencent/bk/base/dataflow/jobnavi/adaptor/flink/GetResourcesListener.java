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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.flink;

import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import org.apache.log4j.Logger;

public class GetResourcesListener implements EventListener {

    private final Logger logger;

    private final FlinkSubmitTask task;

    GetResourcesListener(FlinkSubmitTask task) {
        this.task = task;
        this.logger = task.getLogger();
    }

    /**
     * 根据task或者queue获取空闲资源
     * 1.event参数中不包含queue时，获取task对应的application所在yarn队列的资源；
     * 2.event参数中包含queue时，获取queue的资源情况，防止yarn cluster模式下，application被kill时，获取不到资源的情况。
     *
     * @param event 包含queue的map，queue参数可选
     * @return 空闲资源
     */
    @Override
    public TaskEventResult doEvent(TaskEvent event) {
        TaskEventResult result = new TaskEventResult();
        try {
            String returnValue = JsonUtils.writeValueAsString(YarnUtil.getStreamQueuesResources());
            result.setProcessInfo(returnValue);
        } catch (Throwable e) {
            logger.error("Get queue resources error.", e);
            result.setSuccess(false);
            result.setProcessInfo("Get queue resources error. " + e.getMessage());
        }
        return result;
    }
}
