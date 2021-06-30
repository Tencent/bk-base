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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.http;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class SavePointHandler extends AbstractHttpHandler {

    private static final Logger logger = Logger.getLogger(SavePointHandler.class);

    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        String operate = (String) params.get("operate");
        AbstractJobDao dao = MetaDataManager.getJobDao();
        try {
            String scheduleId = (String) params.get("schedule_id");
            switch (operate) {
                case "save":
                    Long scheduleTime = (Long) params.get("schedule_time");
                    String savePointString = JsonUtils.writeValueAsString(params.get("savepoint"));
                    dao.saveSavepoint(scheduleId, scheduleTime, savePointString);
                    writeData(true, "save point success.", response);
                    break;
                case "save_schedule":
                    savePointString = JsonUtils.writeValueAsString(params.get("savepoint"));
                    dao.saveSavepoint(scheduleId, savePointString);
                    writeData(true, "save point success.", response);
                    break;
                case "get":
                    scheduleTime = (Long) params.get("schedule_time");
                    String savepoint = dao.getSavepoint(scheduleId, scheduleTime);
                    writeData(true, "get savepoint success.", HttpReturnCode.DEFAULT, savepoint, response);
                    break;
                case "get_schedule":
                    savepoint = dao.getSavepoint(scheduleId);
                    writeData(true, "get savepoint success.", HttpReturnCode.DEFAULT, savepoint, response);
                    break;
                case "delete":
                    scheduleTime = (Long) params.get("schedule_time");
                    dao.deleteSavepoint(scheduleId, scheduleTime);
                    writeData(true, "delete savepoint success.", HttpReturnCode.DEFAULT, null, response);
                    break;
                case "delete_schedule":
                    dao.deleteSavepoint(scheduleId);
                    writeData(true, "delete savepoint success.", HttpReturnCode.DEFAULT, null, response);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            logger.error("save savepoint error.", e);
            writeData(false, "save savepoint error: " + e.getMessage(), response);
        }
    }
}
