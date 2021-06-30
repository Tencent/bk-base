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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.decommission;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DecommissionEventResult;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DecommissionEventStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import org.apache.log4j.Logger;

public class RunnerTaskRecoveryThread implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RunnerTaskRecoveryThread.class);
    private final RunnerInfo info;

    public RunnerTaskRecoveryThread(RunnerInfo info) {
        this.info = info;
    }

    @Override
    public void run() {
        LOGGER.info(String.format("recovery decommissioned runner %s task starting", info.getRunnerId()));
        for (DecommissionEventResult result : info.getDecommissioningTasks().values()) {
            long executeId = TaskStateManager.newExecuteId();
            if (isRecovery(result.getStatus())) {
                try {
                    TaskStateManager.dispatchEvent(
                            DefaultTaskEventBuilder.buildEvent(TaskStatus.preparing.toString(),
                                    executeId,
                                    null,
                                    result.getTaskInfo()));
                } catch (NaviException e) {
                    LOGGER.error(e);
                }
            }
        }
        LOGGER.info(String.format("recovery decommissioned runner %s task finished", info.getRunnerId()));
    }

    private boolean isRecovery(DecommissionEventStatus status) {
        switch (status) {
            case decommissioned:
            case failed:
                return true;
            case decommissioning:
            case lost:
                return false;
            default:
                return true;
        }
    }
}
