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

package com.tencent.bk.base.dataflow.jobnavi.runner.task.process;

import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.callback.AbstractTaskEventProcessCallback;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.callback.CustomTaskEventCallback;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.callback.DecommissionEventCallback;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.runner.task.process.callback.RunningTaskEventCallback;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingTransport;

public class TaskEventProcessCallBackFactory {

    public static Map<String, Class<? extends AbstractTaskEventProcessCallback>> callbackMaps = new HashMap<>();

    static {
        register("running", RunningTaskEventCallback.class);
        register("decommission", DecommissionEventCallback.class);
    }

    private static void register(String eventName, Class<? extends AbstractTaskEventProcessCallback> callback) {
        callbackMaps.put(eventName, callback);
    }

    /**
     * generate task event process callback
     * @param conf
     * @param eventInfo
     * @param transport
     * @param clientManager
     * @return task event process callback
     * @throws NaviException
     */
    static AbstractTaskEventProcessCallback generateProcessCallBack(Configuration conf, TaskEventInfo eventInfo,
            TNonblockingTransport transport, TAsyncClientManager clientManager) throws NaviException {
        Class<? extends AbstractTaskEventProcessCallback> clz
                = callbackMaps.get(eventInfo.getTaskEvent().getEventName());
        if (clz == null) {
            clz = CustomTaskEventCallback.class;
        }
        try {
            Constructor<? extends AbstractTaskEventProcessCallback> c = clz
                    .getConstructor(Configuration.class, TaskEventInfo.class, TNonblockingTransport.class,
                            TAsyncClientManager.class);
            return c.newInstance(conf, eventInfo, transport, clientManager);
        } catch (InstantiationException
                | IllegalAccessException
                | NoSuchMethodException
                | InvocationTargetException e) {
            throw new NaviException(e);
        }
    }

}
