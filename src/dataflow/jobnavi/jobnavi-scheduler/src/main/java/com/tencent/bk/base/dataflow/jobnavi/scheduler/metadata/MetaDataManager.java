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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.JobNaviScheduler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import org.apache.log4j.Logger;

public class MetaDataManager {

    private static final Logger logger = Logger.getLogger(JobNaviScheduler.class);

    private static AbstractJobDao jobdao;

    /**
     * get job dao
     *
     * @return
     */
    public static AbstractJobDao getJobDao() {
        if (jobdao == null) {
            throw new NullPointerException("JobDao may not init");
        }
        return jobdao;
    }

    /**
     * init meta data manager
     *
     * @param conf Scheduler Config
     * @throws NaviException init Error
     */
    public static synchronized void init(Configuration conf) throws NaviException {
        logger.info("init JobDao...");
        String sqlDaoClassName = conf
                .getString(Constants.JOBNAVI_SCHEDULER_JOBDAO_CLASS, Constants.JOBNAVI_SCHEDULER_JOBDAO_CLASS_DEFAULT);
        try {
            jobdao = (AbstractJobDao) Class.forName(sqlDaoClassName).newInstance();
            jobdao.init(conf);

            String version = conf.getString(Constants.JOBNAVI_VERSION);
            String dbVersion = jobdao.getCurrentVersion();
            logger.info("code version is " + version + " and db version is " + dbVersion);

            if (version.compareTo(dbVersion) > 0) {
                logger.info("Upgrade version to:" + version);
                jobdao.upgrade(version, dbVersion);
            } else if (version.compareTo(dbVersion) < 0) {
                throw new NaviException("Please use the latest version code.");
            }
        } catch (ClassNotFoundException e) {
            logger.error("Can not find JobDao: " + sqlDaoClassName, e);
            throw new NaviException(e);
        } catch (IllegalAccessException | InstantiationException e) {
            logger.error("Create class " + sqlDaoClassName + " error.", e);
            throw new NaviException(e);
        }
    }
}
