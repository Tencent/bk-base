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

package com.tencent.bk.base.datahub.databus.commons.utils;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * LogUtils Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/14/2018</pre>
 */
public class LogUtilsTest {

    private static Logger logger = LoggerFactory.getLogger(LogUtilsTest.class);


    /**
     * Method: trace(Logger log, String format, Object arg1)
     */
    @Test
    public void testConstructor() throws Exception {
        LogUtils logUtils = new LogUtils();
        assertNotNull(logUtils);
    }

    /**
     * Method: trace(Logger log, String format, Object arg1)
     */
    @Test
    public void testTraceForLogFormatArg1() throws Exception {
        LogUtils.trace(logger, "method {} occurs an error!", "test");
    }

    /**
     * Method: trace(Logger log, String format, Object arg1, Object arg2)
     */
    @Test
    public void testTraceForLogFormatArg1Arg2() throws Exception {
        LogUtils.trace(logger, "class{} method {} occurs an error!", new Object[]{"testclass", "testmethod"});
    }

    /**
     * Method: trace(Logger log, String format, Object... argArray)
     */
    @Test
    public void testTraceForLogFormatArgArray() throws Exception {
        LogUtils.trace(logger, "class{} method {} occurs an error!", "testclass", "testmethod");
    }

    /**
     * Method: debug(Logger log, String format, Object arg1)
     */
    @Test
    public void testDebugForLogFormatArg1() throws Exception {
        LogUtils.debug(logger, "method {} occurs an error!", "testmethod");
    }

    /**
     * Method: debug(Logger log, String format, Object arg1, Object arg2)
     */
    @Test
    public void testDebugForLogFormatArg1Arg2() throws Exception {
        LogUtils.debug(logger, "class {} method {} occurs an error!", new Object[]{"testclass", "testmethod"});
    }

    /**
     * Method: debug(Logger log, String format, Object... argArray)
     */
    @Test
    public void testDebugForLogFormatArgArray() throws Exception {
        LogUtils.debug(logger, "class {} method {} occurs an error!", "testclass", "testmethod");
    }

    /**
     * Method: info(Logger log, String format, Object arg1)
     */
    @Test
    public void testInfoForLogFormatArg1() throws Exception {
        LogUtils.info(logger, "method {} occurs an error!", "testmethod");
    }

    /**
     * Method: info(Logger log, String format, Object arg1, Object arg2)
     */
    @Test
    public void testInfoForLogFormatArg1Arg2() throws Exception {
        LogUtils.info(logger, "class {} method {} occurs an error!", new Object[]{"testclass", "testmethod"});
    }

    /**
     * Method: info(Logger log, String format, Object... argArray)
     */
    @Test
    public void testInfoForLogFormatArgArray() throws Exception {
        LogUtils.info(logger, "class {} method {} occurs an error!", "testclass", "testmethod");
    }

    /**
     * Method: warn(Logger log, String format, Object arg1)
     */
    @Test
    public void testWarnForLogFormatArg1() throws Exception {
        LogUtils.warn(logger, "method {} occurs an error!", "testmethod");
    }

    /**
     * Method: warn(Logger log, String format, Object arg1, Object arg2)
     */
    @Test
    public void testWarnForLogFormatArg1Arg2() throws Exception {
        LogUtils.warn(logger, "class {} method {} occurs an error!", new Object[]{"testclass", "testmethod"});
    }

    /**
     * Method: warn(Logger log, String format, Object... argArray)
     */
    @Test
    public void testWarnForLogFormatArgArray() throws Exception {
        LogUtils.warn(logger, "class {} method {} occurs an error!", "testclass", "testmethod");
    }

    /**
     * Method: warn(Logger log, String msg, Throwable t)
     */
    @Test
    public void testWarnForLogMsgT() throws Exception {
        LogUtils.warn(logger, "warn message!", new Throwable());
    }

    /**
     * Method: error(String errorCode, Logger log, String format, Object arg1)
     */
    @Test
    public void testErrorCodeLogFormatArg1() throws Exception {
        LogUtils.error("-1", logger, "method {} occurs an error!", "testclass");
    }

    /**
     * Method: error(String errorCode, Logger log, String format, Object arg1, Object arg2)
     */
    @Test
    public void testErrorCodeLogFormatArg12() throws Exception {
        LogUtils.error("-1", logger, "class {} method {} occurs an error!", new Object[]{"testClass", "testMethod"});
    }

    /**
     * Method: error(String errorCode, Logger log, String format, Object... argArray)
     */
    @Test
    public void testErrorCodeLogFormatArgArray() throws Exception {
        LogUtils.error("-1", logger, "class {} method {} occurs an error!", "testClass", "testMethod");
    }

    /**
     * Method: error(String errorCode, Logger log, String msg, Throwable t)
     */
    @Test
    public void testErrorForErrorCodeLogMsgT() throws Exception {
        LogUtils.error("-1", logger, "class xx method xx occurs an error!", new Throwable());
    }


} 
