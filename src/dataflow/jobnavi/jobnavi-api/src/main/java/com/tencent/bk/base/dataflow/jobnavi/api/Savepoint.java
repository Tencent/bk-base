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

package com.tencent.bk.base.dataflow.jobnavi.api;

import com.tencent.bk.base.dataflow.jobnavi.ha.HAProxy;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.HashMap;
import java.util.Map;

public class Savepoint {

    /**
     * save jobnavi savepoint
     *
     * @param scheduleId
     * @param value
     * @throws Exception
     */
    public static void save(String scheduleId, Map<String, String> value) throws Exception {
        Map<String, Object> param = new HashMap<>();
        param.put("operate", "save_schedule");
        param.put("schedule_id", scheduleId);
        param.put("savepoint", value);
        String paramStr = JsonUtils.writeValueAsString(param);
        HAProxy.sendPostRequest("/sys/savepoint", paramStr, null);
    }

    /**
     * save jobnavi savepoint
     *
     * @param scheduleId
     * @param scheduleTime
     * @param value
     * @throws Exception
     */
    public static void save(String scheduleId, Long scheduleTime, Map<String, String> value) throws Exception {
        Map<String, Object> param = new HashMap<>();
        param.put("operate", "save");
        param.put("schedule_id", scheduleId);
        param.put("schedule_time", scheduleTime);
        param.put("savepoint", value);
        String paramStr = JsonUtils.writeValueAsString(param);
        HAProxy.sendPostRequest("/sys/savepoint", paramStr, null);
    }

    /**
     * get jobnavi savepoint
     *
     * @param scheduleId
     * @return save point data
     * @throws Exception
     */
    public static String get(String scheduleId) throws Exception {
        Map<String, Object> param = new HashMap<>();
        param.put("operate", "get_schedule");
        param.put("schedule_id", scheduleId);
        String paramStr = JsonUtils.writeValueAsString(param);
        return HAProxy.sendPostRequest("/sys/savepoint", paramStr, null);
    }


    /**
     * get jobnavi savepoint
     *
     * @param scheduleId
     * @param scheduleTime
     * @return save point data
     * @throws Exception
     */
    public static String get(String scheduleId, Long scheduleTime) throws Exception {
        Map<String, Object> param = new HashMap<>();
        param.put("operate", "get");
        param.put("schedule_id", scheduleId);
        param.put("schedule_time", scheduleTime);
        String paramStr = JsonUtils.writeValueAsString(param);
        return HAProxy.sendPostRequest("/sys/savepoint", paramStr, null);
    }

    /**
     * delete jobnavi savepoint
     *
     * @param scheduleId
     * @param scheduleTime
     * @return response message
     * @throws Exception
     */
    public static String delete(String scheduleId, Long scheduleTime) throws Exception {
        Map<String, Object> param = new HashMap<>();
        param.put("operate", "delete");
        param.put("schedule_id", scheduleId);
        param.put("schedule_time", scheduleTime);
        String paramStr = JsonUtils.writeValueAsString(param);
        return HAProxy.sendPostRequest("/sys/savepoint", paramStr, null);
    }

    /**
     * delete jobnavi savepoint
     *
     * @param scheduleId
     * @return response message
     * @throws Exception
     */
    public static String delete(String scheduleId) throws Exception {
        Map<String, Object> param = new HashMap<>();
        param.put("operate", "delete_schedule");
        param.put("schedule_id", scheduleId);
        String paramStr = JsonUtils.writeValueAsString(param);
        return HAProxy.sendPostRequest("/sys/savepoint", paramStr, null);
    }
}
