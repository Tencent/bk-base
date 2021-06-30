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

package tencent.bk.base.jobnavi.scheduler.service;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.service.HttpServerService;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHttpServerService {

    private Configuration conf;

    /**
     * init test
     */
    @Before
    public void init() {
        InputStream is = this.getClass().getResourceAsStream("/jobnavi.properties");
        try {
            conf = new Configuration(is);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        Thread httpServer = new Thread() {
            @Override
            public void run() {
                try {
                    HttpServerService service = new HttpServerService();
                    service.start(conf);
                } catch (NaviException e) {
                    Assert.fail(e.getMessage());
                }
            }
        };
        httpServer.start();
    }

    @Test
    public void testHTTPServerService() {
        try {
            Thread.sleep(3000); // waiting for http server start
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }

        try {
            String result = HttpUtils.get("http://0.0.0.0:" + conf
                    .getInt(Constants.JOBNAVI_SCHEDULER_HTTP_PORT, Constants.JOBNAVI_SCHEDULER_HTTP_PORT_DEFAULT)
                                                  + "/healthz");
            Assert.assertEquals("ok", result);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

}
