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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyRule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class ScheduleManager {

    private static final Logger logger = Logger.getLogger(ScheduleManager.class);

    private static Schedule schedule;
    private static boolean ready = false;

    /**
     * get schedule
     *
     * @return
     */
    public static Schedule getSchedule() {
        if (schedule == null) {
            throw new NullPointerException("schedule may not init");
        }
        return schedule;
    }

    public static boolean isReady() {
        return ready;
    }

    public static void setReady(boolean ready) {
        ScheduleManager.ready = ready;
    }

    /**
     * init schedule
     *
     * @param conf jobnavi config
     * @throws NaviException init failed
     */
    public static synchronized void init(Configuration conf) throws NaviException {
        logger.info("init Schedule...");
        String scheduleClassname = conf.getString(Constants.JOBNAVI_SCHEDULER_SCHEDULE_CLASS,
                Constants.JOBNAVI_SCHEDULER_SCHEDULE_CLASS_DEFAULT);
        try {
            schedule = (Schedule) Class.forName(scheduleClassname).newInstance();
            schedule.init(conf);
            schedule.start();
            setReady(true);
        } catch (ClassNotFoundException e) {
            logger.error("Cannot find schedule: " + scheduleClassname, e);
            throw new NaviException(e);
        } catch (IllegalAccessException | InstantiationException e) {
            logger.error("Create class " + scheduleClassname + " error.", e);
            throw new NaviException(e);
        }
    }

    /**
     * add schedule info
     *
     * @param info
     * @return execute ID if executed
     * @throws NaviException
     */
    public static synchronized Long addScheduleInfo(ScheduleInfo info) throws NaviException {
        if (isScheduleExist(info.getScheduleId())) {
            throw new NaviException("schedule [" + info.getScheduleId() + "] already exist.");
        }
        validateScheduleInfo(info);
        if (info.getPeriod() != null && info.getPeriod().getFirstScheduleTime() == null) {
            info.getPeriod().setFirstScheduleTime(new Date());
        }
        MetaDataManager.getJobDao().addScheduleInfo(info);
        return schedule.addSchedule(info);
    }

    public static synchronized void delScheduleInfo(String scheduleId) throws NaviException {
        MetaDataManager.getJobDao().deleteScheduleInfo(scheduleId);
        schedule.deleteSchedule(scheduleId);
    }

    /**
     * update schedule info
     *
     * @param info
     * @throws NaviException
     */
    public static synchronized void updateScheduleInfo(ScheduleInfo info) throws NaviException {
        if (!isScheduleExist(info.getScheduleId())) {
            throw new NaviException("schedule [" + info.getScheduleId() + "] not exist.");
        }
        validateScheduleInfo(info);
        MetaDataManager.getJobDao().updateScheduleInfo(info);
        schedule.updateSchedule(info);
    }

    /**
     * overwrite schedule info
     *
     * @param info
     * @return execute ID if executed
     * @throws NaviException
     */
    public static synchronized Long overwriteScheduleInfo(ScheduleInfo info) throws NaviException {
        validateScheduleInfo(info);
        MetaDataManager.getJobDao().deleteScheduleInfo(info.getScheduleId());
        schedule.deleteSchedule(info.getScheduleId());
        MetaDataManager.getJobDao().addScheduleInfo(info);
        return schedule.addSchedule(info);
    }

    /**
     * active schedule job
     *
     * @param scheduleId
     * @param active
     * @throws NaviException
     */
    public static synchronized void activeScheduleInfo(String scheduleId, boolean active) throws NaviException {
        if (!isScheduleExist(scheduleId)) {
            throw new NaviException("schedule [" + scheduleId + "] not exist.");
        }
        MetaDataManager.getJobDao().activeSchedule(scheduleId, active);
        if (active) {
            schedule.startScheduleJob(scheduleId);
        } else {
            schedule.stopScheduleJob(scheduleId);
        }
    }

    public static void stop() throws NaviException {
        if (schedule != null) {
            schedule.stop();
        }
    }

    private static boolean isScheduleExist(String scheduleId) {
        return scheduleId != null && schedule.getScheduleInfo(scheduleId) != null;
    }


    private static void validateScheduleInfo(ScheduleInfo info) throws NaviException {
        if (StringUtils.isEmpty(info.getScheduleId())) {
            throw new NaviException("schedule_id can not be null.");
        }

        if (StringUtils.isEmpty(info.getTypeId())) {
            throw new NaviException("type_id can not be null.");
        }

        List<DependencyInfo> parents = info.getParents();
        if (parents != null) {
            List<DependencyInfo> needCheckParentDependencyList = new ArrayList<>();
            for (DependencyInfo parent : parents) {
                if (parent.getRule() != DependencyRule.self_finished
                        && parent.getRule() != DependencyRule.self_no_failed) {
                    needCheckParentDependencyList.add(parent);
                }
            }

            if (needCheckParentDependencyList.size() > 0 && isParentNameRecycle(info.getScheduleId(),
                    needCheckParentDependencyList)) {
                throw new NaviException("Parent depend recycle.");
            }
        }
    }

    private static boolean isParentNameRecycle(String currentId, List<DependencyInfo> parents) {
        if (isScheduleIdContain(currentId, parents)) {
            return true;
        }

        for (DependencyInfo parent : parents) {
            ScheduleInfo parentInfo = schedule.getScheduleInfo(parent.getParentId());
            if (parentInfo == null) {
                continue;
            }

            List<DependencyInfo> ppNames = parentInfo.getParents();
            if (ppNames == null || ppNames.size() == 0) {
                continue;
            }
            List<DependencyInfo> needCheckParentDependencyList = new ArrayList<>();
            for (DependencyInfo ppName : ppNames) {
                if (ppName.getRule() != DependencyRule.self_finished
                        && ppName.getRule() != DependencyRule.self_no_failed) {
                    needCheckParentDependencyList.add(ppName);
                }
            }

            if (isParentNameRecycle(currentId, needCheckParentDependencyList)) {
                return true;
            }
        }

        return false;
    }

    private static boolean isScheduleIdContain(String currentName, List<DependencyInfo> parents) {
        for (DependencyInfo parent : parents) {
            if (currentName.equals(parent.getParentId())) {
                return true;
            }
        }
        return false;
    }
}
