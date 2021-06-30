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

package com.tencent.bk.base.dataflow.codecheck.http;

import static org.mockito.Mockito.verify;

import com.tencent.bk.base.dataflow.codecheck.util.BlackListGroup;
import com.tencent.bk.base.dataflow.codecheck.util.Constants;
import com.tencent.bk.base.dataflow.codecheck.util.HttpReturnCode;
import com.tencent.bk.base.dataflow.codecheck.util.JsonUtils;
import com.tencent.bk.base.dataflow.codecheck.util.db.CodeCheckDBUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BlackListGroup.class, JsonUtils.class, CodeCheckHandler.class,
        Request.class, Response.class, CodeCheckDBUtil.class})
public class BlacklistGroupOpHandlerTest {

    private AutoCloseable closeable;

    @Before
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @After
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void doGetOpAdd() throws Exception {
        BlacklistGroupOpHandler blacklistGroupOpHandler = PowerMockito.mock(BlacklistGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String blacklistGroupName = "aa";
        String blacklistOpName = "add";
        String blacklist = "a.b;c.d.e";
        PowerMockito.when(request.getParameter("blacklist_group_name")).thenReturn(blacklistGroupName);
        PowerMockito.when(request.getParameter("blacklist_op")).thenReturn(blacklistOpName);
        PowerMockito.when(request.getParameter("blacklist")).thenReturn(blacklist);

        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);
        PowerMockito.doNothing().when(blackListGroup).addBlackListGroup(blacklistGroupName, blacklist);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.doNothing().when(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addBlacklistGroup(blacklistGroupName, blacklist);

        PowerMockito.doCallRealMethod().when(blacklistGroupOpHandler).doGet(request, response);
        blacklistGroupOpHandler.doGet(request, response);

        Map<String, Object> retJson = new HashMap<>();
        retJson.put(Constants.BLACKLIST_OP, blacklistOpName);
        retJson.put(Constants.BLACKLIST_GROUP_NAME, blacklistGroupName);
        retJson.put(Constants.BLACKLIST, blacklist);

        retJson.put("status", "success");
        verify(blacklistGroupOpHandler).writeData(true, "", HttpReturnCode.DEFAULT,
                retJson, response);
    }

    @Test
    public void doGetOpAddException() throws Exception {
        BlacklistGroupOpHandler blacklistGroupOpHandler = PowerMockito.mock(BlacklistGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String blacklistGroupName = "aa";
        String blacklistOpName = "add";
        String blacklist = "a.b;c.d.e";
        PowerMockito.when(request.getParameter("blacklist_group_name")).thenReturn(blacklistGroupName);
        PowerMockito.when(request.getParameter("blacklist_op")).thenReturn(blacklistOpName);
        PowerMockito.when(request.getParameter("blacklist")).thenReturn(blacklist);

        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);
        String exceptionMsg = "exception msg";
        PatternSyntaxException patternSyntaxException =
                new PatternSyntaxException(exceptionMsg, exceptionMsg, 1);
        PowerMockito.doThrow(patternSyntaxException).when(blackListGroup)
                .addBlackListGroup(blacklistGroupName, blacklist);

        PowerMockito.doCallRealMethod().when(blacklistGroupOpHandler).doGet(request, response);
        blacklistGroupOpHandler.doGet(request, response);

        verify(blacklistGroupOpHandler).writeData(false, patternSyntaxException.getMessage(),
                HttpReturnCode.DEFAULT, response);
    }

    @Test
    public void doGetOpDelete() throws Exception {
        BlacklistGroupOpHandler blacklistGroupOpHandler = PowerMockito.mock(BlacklistGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String blacklistGroupName = "aa";
        String blacklistOpName = "delete";
        PowerMockito.when(request.getParameter("blacklist_group_name")).thenReturn(blacklistGroupName);
        PowerMockito.when(request.getParameter("blacklist_op")).thenReturn(blacklistOpName);

        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);
        PowerMockito.doNothing().when(blackListGroup).deleteBlackListGroup(blacklistGroupName);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.doNothing().when(CodeCheckDBUtil.class);
        CodeCheckDBUtil.deleteBlacklistGroup(blacklistGroupName);

        PowerMockito.doCallRealMethod().when(blacklistGroupOpHandler).doGet(request, response);
        blacklistGroupOpHandler.doGet(request, response);

        Map<String, Object> retJson = new HashMap<>();
        retJson.put(Constants.BLACKLIST_OP, blacklistOpName);
        retJson.put(Constants.BLACKLIST_GROUP_NAME, blacklistGroupName);

        retJson.put("status", "success");
        verify(blacklistGroupOpHandler).writeData(true, "", HttpReturnCode.DEFAULT,
                retJson, response);
    }

    @Test
    public void doGetOpList() throws Exception {
        BlacklistGroupOpHandler blacklistGroupOpHandler = PowerMockito.mock(BlacklistGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String blacklistGroupName = "aa";
        String blacklistOpName = "list";
        PowerMockito.when(request.getParameter("blacklist_group_name")).thenReturn(blacklistGroupName);
        PowerMockito.when(request.getParameter("blacklist_op")).thenReturn(blacklistOpName);

        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);
        Map<String, List<String>> blackListGroupInfo = new HashMap<>();
        PowerMockito.when(blackListGroup.listBlackListGroup(blacklistGroupName)).thenReturn(blackListGroupInfo);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.doNothing().when(CodeCheckDBUtil.class);
        CodeCheckDBUtil.deleteBlacklistGroup(blacklistGroupName);

        PowerMockito.doCallRealMethod().when(blacklistGroupOpHandler).doGet(request, response);
        blacklistGroupOpHandler.doGet(request, response);

        Map<String, Object> retJson = new HashMap<>();
        retJson.put(Constants.BLACKLIST_OP, blacklistOpName);
        retJson.put(Constants.BLACKLIST_GROUP_NAME, blacklistGroupName);
        retJson.put("result", blackListGroupInfo);

        retJson.put("status", "success");
        verify(blacklistGroupOpHandler).writeData(true, "", HttpReturnCode.DEFAULT,
                retJson, response);
    }

    @Test
    public void doGetOpOther() throws Exception {
        BlacklistGroupOpHandler blacklistGroupOpHandler = PowerMockito.mock(BlacklistGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String blacklistGroupName = "aa";
        String blacklistOpName = "other_op";
        PowerMockito.when(request.getParameter("blacklist_group_name")).thenReturn(blacklistGroupName);
        PowerMockito.when(request.getParameter("blacklist_op")).thenReturn(blacklistOpName);

        PowerMockito.doCallRealMethod().when(blacklistGroupOpHandler).doGet(request, response);
        blacklistGroupOpHandler.doGet(request, response);

        String blacklistGroupMessage = "not supported op";
        verify(blacklistGroupOpHandler).writeData(false, blacklistGroupMessage, HttpReturnCode.DEFAULT,
                response);
    }
}