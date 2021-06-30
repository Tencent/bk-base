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

package tencent.bk.base.jobnavi.scheduler.metadata.mysql;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tencent.bk.base.jobnavi.scheduler.metadata.mysql.util.MetaDataTestUtil;

public class TestMetaDataManager {

    private Configuration conf;

    @Before
    public void before() {
        try {
            conf = MetaDataTestUtil.initConfig();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        try {
            MetaDataTestUtil.cleanTestDatabaseTables(conf);
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMetaDataManager() {
        try {
            MetaDataManager.init(conf);
            AbstractJobDao jobDao = MetaDataManager.getJobDao();
            Assert.assertNotEquals(Constants.JOBNAVI_NULL_VERSION, jobDao.getCurrentVersion());
        } catch (NaviException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @After
    public void after() {
        try {
            MetaDataTestUtil.cleanTestDatabaseTables(conf);
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }
}
