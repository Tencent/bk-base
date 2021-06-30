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
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyConstants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.DependencyManager;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class SchedulerHandler extends AbstractHttpHandler {

    private static final Logger logger = Logger.getLogger(SchedulerHandler.class);

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String operate = request.getParameter("operate");
        String scheduleId = request.getParameter("schedule_id");
        if (scheduleId == null) {
            logger.error("[schedule_id] is required");
            writeData(false, "[schedule_id] is required", HttpReturnCode.ERROR_PARAM_MISSING, response);
            return;
        }
        ScheduleInfo info = ScheduleManager.getSchedule().getScheduleInfo(scheduleId);
        switch (operate) {
            case "delete":
                try {
                    ScheduleManager.delScheduleInfo(scheduleId);
                    writeData(true, "delete schedule [" + scheduleId + "] success.", response);
                } catch (Throwable e) {
                    logger.error("delete schedule [" + scheduleId + "] failed.", e);
                    writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
                }
                break;
            case "disable":
                boolean disable = false;
                if ("1".equals(request.getParameter("disable"))) {
                    disable = true;
                }
                try {
                    ScheduleManager.activeScheduleInfo(scheduleId, !disable);
                    writeData(true, "disable schedule [" + scheduleId + "] success.", response);
                } catch (Throwable e) {
                    logger.error("disable schedule [" + scheduleId + "] failed.", e);
                    writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
                }
                break;
            case "get":
                if (info == null) {
                    logger.error("Can not find schedule [" + scheduleId + "]");
                    writeData(false, "Can not find schedule [" + scheduleId + "]", HttpReturnCode.ERROR_PARAM_INVALID,
                            response);
                } else {
                    writeData(true, "", HttpReturnCode.DEFAULT, JsonUtils.writeValueAsString(info.toHTTPJson()),
                            response);
                }
                break;
            case "force_schedule":
                if (info == null) {
                    logger.error("Can not find schedule [" + scheduleId + "]");
                    writeData(false, "Can not find schedule [" + scheduleId + "]", HttpReturnCode.ERROR_PARAM_INVALID,
                            response);
                } else {
                    try {
                        Long executeId = ScheduleManager.getSchedule().forceScheduleTask(scheduleId);
                        writeData(true, "force schedule success.", HttpReturnCode.DEFAULT, String.valueOf(executeId),
                                response);
                    } catch (Exception e) {
                        logger.error("force schedule task error.", e);
                        writeData(false, "force schedule error.", HttpReturnCode.ERROR_RUNTIME_EXCEPTION,
                                e.getMessage(), response);
                    }
                }
                break;
            case "get_execute_lifespan":
                if (info == null) {
                    logger.error("Can not find schedule [" + scheduleId + "]");
                    writeData(false, "Can not find schedule [" + scheduleId + "]", HttpReturnCode.ERROR_PARAM_INVALID,
                            response);
                    return;
                }
                AbstractJobDao dao = MetaDataManager.getJobDao();
                List<String> children = dao.getChildrenByParentId(scheduleId);
                long lifespan = 0;
                for (String child : children) {
                    ScheduleInfo childInfo = ScheduleManager.getSchedule().getScheduleInfo(child);
                    DependencyInfo dependencyInfo = null;
                    for (DependencyInfo parent : childInfo.getParents()) {
                        if (scheduleId.equals(parent.getParentId())) {
                            dependencyInfo = parent;
                        }
                    }
                    if (dependencyInfo == null) {
                        continue;
                    }
                    long delay = 0;
                    if (childInfo.getPeriod() != null && childInfo.getPeriod().getDelay() != null) {
                        delay = CronUtil.parsePeriodStringToMills(childInfo.getPeriod().getDelay());
                    }
                    long windowOffset = DependencyManager.parseWindowOffset(dependencyInfo.getWindowOffset());
                    long windowSize = 0;
                    switch (dependencyInfo.getType()) {
                        case range:
                            windowSize = 3600 * 1000;//1 hour
                            break;
                        case fixed:
                            windowSize = CronUtil.parsePeriodStringToMills(dependencyInfo.getValue());
                            break;
                        case accumulate:
                            //params: $accumulate_start_time:$accumulate_cycle:$begin~$end
                            String[] params = dependencyInfo.getValue()
                                    .split(DependencyConstants.DEPENDENCY_ACCUMULATE_PARAM_SPLIT);
                            if (params.length != 3) {
                                writeData(false, "Invalid accumulate dependency param:" + dependencyInfo.getValue(),
                                        HttpReturnCode.ERROR_PARAM_INVALID, response);
                                return;
                            }
                            windowSize = CronUtil.parsePeriodStringToMills(params[1]);
                            break;
                        default:
                            writeData(false, "Unsupported dependence type:" + dependencyInfo.getType(),
                                      HttpReturnCode.ERROR_PARAM_INVALID, response);
                            return;
                    }
                    long tempLifespan = delay + windowOffset + windowSize;
                    if (tempLifespan > lifespan) {
                        lifespan = tempLifespan;
                    }
                }
                writeData(true, "get execute lifespan of schedule:" + scheduleId + " success.", HttpReturnCode.DEFAULT,
                        String.valueOf(lifespan), response);
                break;
            default:
                throw new IllegalArgumentException("operate " + operate + " is not support.");
        }
    }

    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        String operate = (String) params.get("operate");
        String paramString = JsonUtils.writeValueAsString(params);
        logger.info("schedule operate param: " + paramString);
        if ("add".equals(operate)) {
            ScheduleInfo info = new ScheduleInfo();
            info.parseJson(params);
            try {
                logger.info("add schedule [" + info.getScheduleId() + "] : " + JsonUtils
                        .writeValueAsString(info.toHTTPJson()));
                Long executeId = ScheduleManager.addScheduleInfo(info);
                if (executeId != null) {
                    writeData(true, "add schedule [" + info.getScheduleId() + "] success. execute_id is: " + executeId,
                            HttpReturnCode.DEFAULT, executeId.toString(), response);
                } else {
                    writeData(true, "add schedule [" + info.getScheduleId() + "] success.", response);
                }
            } catch (Throwable e) {
                logger.error("add schedule [" + info.getScheduleId() + "] failed.", e);
                writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else if ("update".equals(operate)) {
            if (params.get("schedule_id") == null) {
                logger.error("[schedule_id] required if update schedule info");
                writeData(false, "[schedule_id] required if update schedule info", HttpReturnCode.ERROR_PARAM_MISSING,
                        response);
                return;
            }

            String scheduleId = params.get("schedule_id").toString();
            ScheduleInfo info = ScheduleManager.getSchedule().getScheduleInfo(scheduleId);
            if (info == null) {
                logger.error("Can not find schedule [" + scheduleId + "]");
                writeData(false, "Can not find schedule [" + scheduleId + "]", HttpReturnCode.ERROR_PARAM_INVALID,
                        response);
                return;
            }
            ScheduleInfo newInfo = info.clone();
            newInfo.parseJson(params);
            try {
                logger.info("update schedule [" + info.getScheduleId() + "] : " + JsonUtils
                        .writeValueAsString(newInfo.toHTTPJson()));
                ScheduleManager.updateScheduleInfo(newInfo);
                writeData(true, "update schedule [" + newInfo.getScheduleId() + "] success.", response);
            } catch (Throwable e) {
                logger.error("update schedule [" + newInfo.getScheduleId() + "] failed.", e);
                writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else if ("overwrite".equals(operate)) {
            ScheduleInfo info = new ScheduleInfo();
            info.parseJson(params);
            try {
                logger.info("overwrite schedule [" + info.getScheduleId() + "] : " + JsonUtils
                        .writeValueAsString(info.toHTTPJson()));
                Long executeId = ScheduleManager.overwriteScheduleInfo(info);
                if (executeId != null) {
                    writeData(true,
                            "overwrite schedule [" + info.getScheduleId() + "] success. execute_id is: " + executeId,
                            HttpReturnCode.DEFAULT, executeId.toString(), response);
                } else {
                    writeData(true, "overwrite schedule [" + info.getScheduleId() + "] success.", response);
                }
            } catch (Throwable e) {
                logger.error("overwrite schedule [" + info.getScheduleId() + "] failed.", e);
                writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else {
            throw new IllegalArgumentException("operate " + operate + " is not support.");
        }
    }
}
