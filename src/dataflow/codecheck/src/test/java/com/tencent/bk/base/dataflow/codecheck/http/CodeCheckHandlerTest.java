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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.verifyPrivate;

import com.github.javaparser.ParseProblemException;
import com.tencent.bk.base.dataflow.codecheck.MethodParser;
import com.tencent.bk.base.dataflow.codecheck.MethodParserGroup;
import com.tencent.bk.base.dataflow.codecheck.MethodParserResult;
import com.tencent.bk.base.dataflow.codecheck.util.BlackListGroup;
import com.tencent.bk.base.dataflow.codecheck.util.HttpReturnCode;
import com.tencent.bk.base.dataflow.codecheck.util.JsonUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
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
@PrepareForTest({BlackListGroup.class, JsonUtils.class, CodeCheckHandler.class, MethodParserGroup.class,
        MethodParser.class, Request.class, Response.class})
public class CodeCheckHandlerTest {

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
    public void testDoPostPython() throws Exception {
        CodeCheckHandler codeCheckHandler = PowerMockito.mock(CodeCheckHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);

        String groupName = "test_py_parser";
        String blacklistName = "test_py_blacklist";
        List<String> checkContent = new ArrayList<String>() {{
            add("a.b");
            add("x.y.z");
        }};
        StringBuilder sb = new StringBuilder();
        for (String str : checkContent) {
            sb.append(str);
            sb.append(";");
        }
        sb.deleteCharAt(sb.length() - 1);
        String checkContentStr = sb.toString();
        Map<String, Object> param = new HashMap() {{
            put("code_language", "python");
            put("check_content", checkContentStr);
            put("parser_group_name", groupName);
            put("blacklist_group_name", blacklistName);
        }};

        PowerMockito.mockStatic(JsonUtils.class);
        InputStream is = PowerMockito.mock(InputStream.class);
        PowerMockito.doReturn(is).when(request).getInputStream();
        PowerMockito.doReturn(param).when(JsonUtils.class, "readMap", is);

        BlackListGroup blacklist = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blacklist);
        List<String> parseResult = new ArrayList<>();
        PowerMockito.when(blacklist.filterPythonImportInBlackList(groupName, checkContent)).thenReturn(parseResult);

        PowerMockito.when(codeCheckHandler, "checkPythonCode", anyList(), anyString(), any(), anyBoolean())
                .thenCallRealMethod();
        PowerMockito.doCallRealMethod().when(codeCheckHandler).doPost(request, response);
        codeCheckHandler.doPost(request, response);

        verifyPrivate(codeCheckHandler).invoke("checkPythonCode", checkContent, blacklistName,
                response, true);

        String parseMessage = "python codecheck success";
        Map<String, Object> retJson = new HashMap<>();
        retJson.put("parse_status", "success");
        retJson.put("parse_result", parseResult);
        retJson.put("parse_message", parseMessage);

        verify(codeCheckHandler).writeData(true, parseMessage, HttpReturnCode.DEFAULT,
                retJson, response);
    }

    @Test
    public void testDoPostPythonFail() throws Exception {
        CodeCheckHandler codeCheckHandler = PowerMockito.mock(CodeCheckHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);

        String groupName = "test_py_parser";
        String blacklistName = "";
        List<String> checkContent = new ArrayList<String>() {{
            add("a.b");
            add("x.y.z");
        }};
        StringBuilder sb = new StringBuilder();
        for (String str : checkContent) {
            sb.append(str);
            sb.append(";");
        }
        sb.deleteCharAt(sb.length() - 1);
        String checkContentStr = sb.toString();
        Map<String, Object> param = new HashMap() {{
            put("code_language", "python");
            put("check_content", checkContentStr);
            put("parser_group_name", groupName);
            put("blacklist_group_name", blacklistName);
        }};

        PowerMockito.mockStatic(JsonUtils.class);
        InputStream is = PowerMockito.mock(InputStream.class);
        PowerMockito.doReturn(is).when(request).getInputStream();
        PowerMockito.doReturn(param).when(JsonUtils.class, "readMap", is);

        PowerMockito.doCallRealMethod().when(codeCheckHandler).doPost(request, response);
        codeCheckHandler.doPost(request, response);

        String parseMessage = "param blacklist_group_name should not be empty";
        verify(codeCheckHandler).writeData(false, parseMessage, HttpReturnCode.ERROR_PARAM_INVALID,
                response);
    }

    @PrepareForTest({BlackListGroup.class, JsonUtils.class, CodeCheckHandler.class, MethodParserGroup.class,
            MethodParser.class, Request.class, Response.class})
    @Test
    public void testDoPostJava() throws Exception {
        CodeCheckHandler codeCheckHandler = PowerMockito.mock(CodeCheckHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);

        String parserName = "test_java_parser";
        String blacklistName = "test_java_blacklist";

        String checkContentStr = "import java.util.HashMap;\n"
                + "\n"
                + "public class MapTest {\n"
                + "    public static void main(String[] args) {\n"
                + "        HashMap<String, String> result = new HashMap<>();\n"
                + "        result.put(\"1\", \"1\");\n"
                + "        System.out.println(\"result: \" + result);\n"
                + "    }\n"
                + "}";
        String checkContentStr2 = Base64.encodeBase64String(checkContentStr.getBytes(StandardCharsets.UTF_8));
        Map<String, Object> param = new HashMap() {{
            put("code_language", "java");
            put("check_content", checkContentStr2);
            put("parser_group_name", parserName);
            put("blacklist_group_name", blacklistName);
            put("check_flag", true);
        }};

        PowerMockito.mockStatic(JsonUtils.class);
        InputStream is = PowerMockito.mock(InputStream.class);
        PowerMockito.doReturn(is).when(request).getInputStream();
        PowerMockito.doReturn(param).when(JsonUtils.class, "readMap", is);

        BlackListGroup blacklist = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blacklist);

        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        PowerMockito.mockStatic(MethodParserGroup.class);
        PowerMockito.when(MethodParserGroup.getSingleton()).thenReturn(methodParserGroup);

        MethodParser methodParser = PowerMockito.mock(MethodParser.class);
        PowerMockito.when(methodParserGroup.getParserGroup(parserName)).thenReturn(methodParser);

        List<MethodParserResult> parseResultList = new ArrayList<>();
        parseResultList.add(new MethodParserResult("XXX", "System.out.println", 8));
        PowerMockito.when(methodParser.parseJavaCode(anyString(), anyBoolean())).thenReturn(parseResultList);
        PowerMockito.when(blacklist.filterJavaMethodInBlackList(anyString(), anyList())).thenReturn(parseResultList);

        PowerMockito.when(codeCheckHandler, "checkJavaCode", anyString(), anyString(), anyString(),
                any(), anyBoolean()).thenCallRealMethod();

        PowerMockito.doCallRealMethod().when(codeCheckHandler).doPost(request, response);
        codeCheckHandler.doPost(request, response);

        verifyPrivate(codeCheckHandler).invoke("checkJavaCode",
                anyString(), anyString(), anyString(), any(), anyBoolean());

        String parseMessage = "java codecheck success";
        Map<String, Object> retJson = new HashMap<>();
        retJson.put("parse_status", "success");
        retJson.put("parse_result", parseResultList);
        retJson.put("parse_message", parseMessage);
        verify(codeCheckHandler).writeData(true, parseMessage, HttpReturnCode.DEFAULT,
                retJson, response);
    }

    @PrepareForTest({BlackListGroup.class, JsonUtils.class, CodeCheckHandler.class, MethodParserGroup.class,
            MethodParser.class, Request.class, Response.class})
    @Test
    public void testDoPostJavaFail() throws Exception {
        CodeCheckHandler codeCheckHandler = PowerMockito.mock(CodeCheckHandler.class);
        Request request = PowerMockito.mock(Request.class);
        Response response = PowerMockito.mock(Response.class);

        String parserName = "test_java_parser";
        String blacklistName = "test_java_blacklist";

        String checkContentStr = "import java.util.HashMap;\n"
                + "\n"
                + "public class MapTest {\n"
                + "    public static void main(String[] args) {\n"
                + "        HashMap<String, String> result = new HashMap<>();\n"
                + "        result.put(\"1\", \"1\");\n"
                + "        System.out.println(\"result: \" + result);\n"
                + "    }\n"
                + "}";
        String checkContentStr2 = Base64.encodeBase64String(checkContentStr.getBytes(StandardCharsets.UTF_8));
        Map<String, Object> param = new HashMap() {{
            put("code_language", "java");
            put("check_content", checkContentStr2);
            put("parser_group_name", parserName);
            put("blacklist_group_name", blacklistName);
            put("check_flag", true);
        }};

        PowerMockito.mockStatic(JsonUtils.class);
        InputStream is = PowerMockito.mock(InputStream.class);
        PowerMockito.doReturn(is).when(request).getInputStream();
        PowerMockito.doReturn(param).when(JsonUtils.class, "readMap", is);

        BlackListGroup blacklist = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blacklist);

        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        PowerMockito.mockStatic(MethodParserGroup.class);
        PowerMockito.when(MethodParserGroup.getSingleton()).thenReturn(methodParserGroup);

        MethodParser methodParser = PowerMockito.mock(MethodParser.class);
        PowerMockito.when(methodParserGroup.getParserGroup(parserName)).thenReturn(methodParser);

        String exceptionMsg = "exception throws";
        PowerMockito.when(methodParser.parseJavaCode(anyString(), anyBoolean())).thenThrow(
                new ParseProblemException(new Exception(exceptionMsg)));

        PowerMockito.when(codeCheckHandler, "checkJavaCode", anyString(), anyString(), anyString(),
                any(), anyBoolean()).thenCallRealMethod();

        PowerMockito.doCallRealMethod().when(codeCheckHandler).doPost(request, response);
        codeCheckHandler.doPost(request, response);

        verifyPrivate(codeCheckHandler).invoke("checkJavaCode",
                anyString(), anyString(), anyString(), any(), anyBoolean());

        verify(codeCheckHandler).writeData(false, exceptionMsg, HttpReturnCode.ERROR_PARAM_INVALID,
                response);
    }
}
