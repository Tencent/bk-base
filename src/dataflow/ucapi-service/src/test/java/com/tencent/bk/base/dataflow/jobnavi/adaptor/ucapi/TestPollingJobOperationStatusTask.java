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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.ucapi;

import static org.junit.Assert.assertEquals;

import com.tencent.bk.base.dataflow.jobnavi.adaptor.ucapi.utils.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest(HttpUtils.class)
public class TestPollingJobOperationStatusTask {

    @Test
    public void testRun() throws Exception {
        // test start
        PowerMockito.mockStatic(HttpUtils.class);
        Map<String, Object> param = new HashMap<>();
        param.put("bk_username", "admin");
        Mockito.when(HttpUtils.post("start", param))
                .thenReturn("{\"result\": \"true\", \"data\": {\"operate\": \"start\", \"operate_id\": 123}}");

        Mockito.when(HttpUtils.get("get"))
                .thenReturn("{\"result\": \"true\", \"data\": {\"j123\": \"ACTIVE\"}}");

        TaskInfo taskInfo = PowerMockito.mock(TaskInfo.class);
        TaskEvent taskEvent = PowerMockito.mock(TaskEvent.class);
        EventContext eventContext = PowerMockito.mock(EventContext.class);
        PowerMockito.when(taskEvent.getContext()).thenReturn(eventContext);
        PowerMockito.when(eventContext.getTaskInfo()).thenReturn(taskInfo);
        PowerMockito.when(taskEvent.getContext().getTaskInfo().getExtraInfo())
                .thenReturn("{\"graph_id\": \"g123\", \"job_id\": \"j123\", "
                        + "\"bk_username\": \"admin\", \"operate_job_url\": \"start\", "
                        + "\"get_operate_result_url\": \"get\", \"operate\": \"start\"}");
        new PollingJobOperationStatusTask().run(taskEvent);

        // test stop
        PowerMockito.when(taskEvent.getContext().getTaskInfo().getExtraInfo())
                .thenReturn("{\"graph_id\": \"g123\", \"job_id\": \"j123\", "
                        + "\"bk_username\": \"admin\", \"operate_job_url\": \"stop\", "
                        + "\"get_operate_result_url\": \"get\", \"operate\": \"stop\"}");

        Mockito.when(HttpUtils.post("stop", param))
                .thenReturn("{\"result\": \"true\", \"data\": {\"operate\": \"stop\", \"operate_id\": 234}}");

        Mockito.when(HttpUtils.get("get"))
                .thenReturn("{\"result\": \"true\", \"data\": {\"j123\": null}}");

        new PollingJobOperationStatusTask().run(taskEvent);
    }

    @Test
    public void testGetEventListener() {
        EventListener listener = new PollingJobOperationStatusTask().getEventListener("string");
        assertEquals(null, listener);
    }

}
