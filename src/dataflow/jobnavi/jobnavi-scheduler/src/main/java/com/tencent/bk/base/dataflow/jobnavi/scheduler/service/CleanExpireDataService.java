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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.service;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.ScheduleManager;
import com.tencent.bk.base.dataflow.jobnavi.service.Service;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import org.apache.log4j.Logger;

public class CleanExpireDataService implements Service {

    private static final Logger LOGGER = Logger.getLogger(CleanExpireDataService.class);
    private Thread cleanExpireDataThread = null;

    @Override
    public String getServiceName() {
        return CleanExpireDataService.class.getSimpleName();
    }

    @Override
    public void start(Configuration conf) throws NaviException {
        LOGGER.info("Start clean thread...");
        cleanExpireDataThread = new Thread(new CleanExpireDataThread(conf), "clean-thread");
        cleanExpireDataThread.start();
    }

    @Override
    public void stop(Configuration conf) throws NaviException {
        if (cleanExpireDataThread != null) {
            cleanExpireDataThread.interrupt();
        }
    }

    @Override
    public void recovery(Configuration conf) throws NaviException {

    }

    static class CleanExpireDataThread implements Runnable {

        Configuration conf;

        CleanExpireDataThread(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                LOGGER.info("clean-thread Interrupted");
            }
            while (!Thread.currentThread().isInterrupted()) {
                LOGGER.info("Clean metadata thread awoke.");
                int expireDay = conf
                        .getInt(Constants.JOBNAVI_METADATA_EXPIRE_DAY, Constants.JOBNAVI_METADATA_EXPIRE_DAY_DEFAULT);
                int minExpireDay = conf.getInt(Constants.JOBNAVI_METADATA_EXPIRE_MIN_DAY,
                        Constants.JOBNAVI_METADATA_EXPIRE_MIN_DAY_DEFAULT);
                if (expireDay < minExpireDay) {
                    expireDay = minExpireDay;
                }
                long now = System.currentTimeMillis();
                long expireMills = (long) expireDay * 24 * 60 * 60 * 1000;
                long expireTimeMills = now - expireMills;
                LOGGER.info("expireTimeMills is " + expireTimeMills + " " + CronUtil.getPrettyTime(expireTimeMills));
                try {
                    MetaDataManager.getJobDao().deleteExpireEvent(expireTimeMills);
                    MetaDataManager.getJobDao().deleteExpireExecute(expireTimeMills);
                    MetaDataManager.getJobDao().deleteExpireRecovery(expireTimeMills);
                    for (String scheduleId : MetaDataManager.getJobDao().listExpireSchedule(expireTimeMills)) {
                        ScheduleManager.delScheduleInfo(scheduleId);
                    }
                    LOGGER.info("clean expire data cost: " + (System.currentTimeMillis() - now));
                } catch (NaviException e) {
                    LOGGER.error("clean expire metadata error.", e);
                }

                try {
                    Thread.sleep(24 * 60 * 60 * 1000);
                } catch (InterruptedException e) {
                    LOGGER.info("clean-thread Interrupted");
                    break;
                }
            }
        }
    }
}
