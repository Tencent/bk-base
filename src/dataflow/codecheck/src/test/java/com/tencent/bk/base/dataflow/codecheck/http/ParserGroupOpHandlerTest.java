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

import com.tencent.bk.base.dataflow.codecheck.MethodParserGroup;
import com.tencent.bk.base.dataflow.codecheck.util.Constants;
import com.tencent.bk.base.dataflow.codecheck.util.HttpReturnCode;
import com.tencent.bk.base.dataflow.codecheck.util.JsonUtils;
import com.tencent.bk.base.dataflow.codecheck.util.db.CodeCheckDBUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
@PrepareForTest({MethodParserGroup.class, JsonUtils.class, CodeCheckHandler.class,
        Request.class, Response.class, CodeCheckDBUtil.class})
public class ParserGroupOpHandlerTest {

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
        ParserGroupOpHandler parserGroupOpHandler = PowerMockito.mock(ParserGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String parserGroupName = "aa";
        String parserOpName = "add";
        String libDir = "/";
        String sourceDir = "/";
        PowerMockito.when(request.getParameter(Constants.PARSER_GROUP_NAME)).thenReturn(parserGroupName);
        PowerMockito.when(request.getParameter(Constants.PARSER_OP)).thenReturn(parserOpName);
        PowerMockito.when(request.getParameter(Constants.LIB_DIR)).thenReturn(libDir);
        PowerMockito.when(request.getParameter(Constants.SOURCE_DIR)).thenReturn(sourceDir);

        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        PowerMockito.mockStatic(MethodParserGroup.class);
        PowerMockito.when(MethodParserGroup.getSingleton()).thenReturn(methodParserGroup);

        PowerMockito.doNothing().when(methodParserGroup).addParserGroup(parserGroupName, libDir, sourceDir);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.doNothing().when(CodeCheckDBUtil.class);
        CodeCheckDBUtil.addParserGroup(parserGroupName, libDir, sourceDir);

        PowerMockito.doCallRealMethod().when(parserGroupOpHandler).doGet(request, response);
        parserGroupOpHandler.doGet(request, response);

        Map<String, Object> retJson = new HashMap<>();
        retJson.put(Constants.PARSER_OP, parserOpName);
        retJson.put(Constants.PARSER_GROUP_NAME, parserGroupName);
        retJson.put(Constants.LIB_DIR, libDir);
        retJson.put(Constants.SOURCE_DIR, sourceDir);

        retJson.put("status", "success");
        verify(parserGroupOpHandler).writeData(true, "", HttpReturnCode.DEFAULT,
                retJson, response);
    }

    @Test
    public void doGetOpAddException() throws Exception {
        ParserGroupOpHandler parserGroupOpHandler = PowerMockito.mock(ParserGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String parserGroupName = "aa";
        String parserOpName = "add";
        String libDir = "/";
        String sourceDir = "/";
        PowerMockito.when(request.getParameter(Constants.PARSER_GROUP_NAME)).thenReturn(parserGroupName);
        PowerMockito.when(request.getParameter(Constants.PARSER_OP)).thenReturn(parserOpName);
        PowerMockito.when(request.getParameter(Constants.LIB_DIR)).thenReturn(libDir);
        PowerMockito.when(request.getParameter(Constants.SOURCE_DIR)).thenReturn(sourceDir);

        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        PowerMockito.mockStatic(MethodParserGroup.class);
        PowerMockito.when(MethodParserGroup.getSingleton()).thenReturn(methodParserGroup);

        String exceptionMsg = "io exception";
        PowerMockito.doThrow(new IOException(exceptionMsg)).when(methodParserGroup)
                .addParserGroup(parserGroupName, libDir, sourceDir);

        PowerMockito.doCallRealMethod().when(parserGroupOpHandler).doGet(request, response);
        parserGroupOpHandler.doGet(request, response);

        verify(parserGroupOpHandler).writeData(false, exceptionMsg, HttpReturnCode.DEFAULT,
                response);
    }

    @Test
    public void doGetOpDelete() throws Exception {
        ParserGroupOpHandler parserGroupOpHandler = PowerMockito.mock(ParserGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String parserGroupName = "aa";
        String parserOpName = "delete";
        PowerMockito.when(request.getParameter(Constants.PARSER_GROUP_NAME)).thenReturn(parserGroupName);
        PowerMockito.when(request.getParameter(Constants.PARSER_OP)).thenReturn(parserOpName);

        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        PowerMockito.mockStatic(MethodParserGroup.class);
        PowerMockito.when(MethodParserGroup.getSingleton()).thenReturn(methodParserGroup);

        PowerMockito.doNothing().when(methodParserGroup).deleteParserGroup(parserGroupName);

        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.doNothing().when(CodeCheckDBUtil.class);
        CodeCheckDBUtil.deleteBlacklistGroup(parserGroupName);

        PowerMockito.doCallRealMethod().when(parserGroupOpHandler).doGet(request, response);
        parserGroupOpHandler.doGet(request, response);

        Map<String, Object> retJson = new HashMap<>();
        retJson.put(Constants.PARSER_OP, parserOpName);
        retJson.put(Constants.PARSER_GROUP_NAME, parserGroupName);
        retJson.put("status", "success");
        verify(parserGroupOpHandler).writeData(true, "", HttpReturnCode.DEFAULT,
                retJson, response);
    }

    @Test
    public void doGetOpList() throws Exception {
        ParserGroupOpHandler parserGroupOpHandler = PowerMockito.mock(ParserGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String parserGroupName = "aa";
        String parserOpName = "list";
        PowerMockito.when(request.getParameter(Constants.PARSER_GROUP_NAME)).thenReturn(parserGroupName);
        PowerMockito.when(request.getParameter(Constants.PARSER_OP)).thenReturn(parserOpName);

        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        PowerMockito.mockStatic(MethodParserGroup.class);
        PowerMockito.when(MethodParserGroup.getSingleton()).thenReturn(methodParserGroup);

        Map<String, Map> parseResult = new HashMap<>();
        PowerMockito.when(methodParserGroup.listParserGroup(parserGroupName)).thenReturn(parseResult);

        PowerMockito.doCallRealMethod().when(parserGroupOpHandler).doGet(request, response);
        parserGroupOpHandler.doGet(request, response);

        Map<String, Object> retJson = new HashMap<>();
        retJson.put(Constants.PARSER_OP, parserOpName);
        retJson.put(Constants.PARSER_GROUP_NAME, parserGroupName);
        retJson.put("result", parseResult);
        retJson.put("status", "success");
        verify(parserGroupOpHandler).writeData(true, "", HttpReturnCode.DEFAULT,
                retJson, response);
    }

    @Test
    public void doGetOpOther() throws Exception {
        ParserGroupOpHandler parserGroupOpHandler = PowerMockito.mock(ParserGroupOpHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);
        String parserGroupName = "aa";
        String parserOpName = "other_op";

        PowerMockito.when(request.getParameter(Constants.PARSER_GROUP_NAME)).thenReturn(parserGroupName);
        PowerMockito.when(request.getParameter(Constants.PARSER_OP)).thenReturn(parserOpName);

        PowerMockito.doCallRealMethod().when(parserGroupOpHandler).doGet(request, response);
        parserGroupOpHandler.doGet(request, response);

        String parserGroupMessage = "not supported op";
        verify(parserGroupOpHandler).writeData(false, parserGroupMessage, HttpReturnCode.DEFAULT,
                response);
    }
}