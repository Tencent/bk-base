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

import com.github.javaparser.ParseProblemException;
import com.google.common.base.Splitter;
import com.tencent.bk.base.dataflow.codecheck.MethodParser;
import com.tencent.bk.base.dataflow.codecheck.MethodParserGroup;
import com.tencent.bk.base.dataflow.codecheck.MethodParserResult;
import com.tencent.bk.base.dataflow.codecheck.util.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.codecheck.util.BlackListGroup;
import com.tencent.bk.base.dataflow.codecheck.util.Constants;
import com.tencent.bk.base.dataflow.codecheck.util.HttpReturnCode;
import com.tencent.bk.base.dataflow.codecheck.util.JsonUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class CodeCheckHandler extends AbstractHttpHandler {

    /**
     * doPost
     *
     * @param request
     * @param response
     */
    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        String codeLanguage = (String) params.get(Constants.CODE_LANGUAGE);
        String checkContent = (String) params.get(Constants.CHECK_CONTENT);
        String decodedJavaCodeFrame = "";
        List<String> pythonImportList = null;
        String parseMessage;
        if ("python".equalsIgnoreCase(codeLanguage)) {
            pythonImportList = Splitter.on(";").trimResults().splitToList(checkContent);
        } else if ("java".equalsIgnoreCase(codeLanguage)) {
            try {
                decodedJavaCodeFrame = new String(Base64.decodeBase64(checkContent), StandardCharsets.UTF_8);
            } catch (Exception ex) {
                parseMessage = "param check_content base64 decode error";
                writeData(false, parseMessage, HttpReturnCode.ERROR_PARAM_INVALID, response);
                return;
            }
        }
        // only check libGroupName for java
        String parserGroupName = (String) params.get(Constants.PARSER_GROUP_NAME);
        // check blacklistGroupName for java & python
        String blacklistGroupName = (String) params.get(Constants.BLACKLIST_GROUP_NAME);
        if (blacklistGroupName == null || blacklistGroupName.trim().isEmpty()) {
            parseMessage = "param blacklist_group_name should not be empty";
            writeData(false, parseMessage, HttpReturnCode.ERROR_PARAM_INVALID, response);
            return;
        }
        boolean checkFlag =
                params.containsKey(Constants.CHECK_FLAG) ? (Boolean) params.get(Constants.CHECK_FLAG) : true;
        if ("java".equalsIgnoreCase(codeLanguage)) {
            checkJavaCode(parserGroupName, decodedJavaCodeFrame, blacklistGroupName, response, checkFlag);
        } else if ("python".equalsIgnoreCase(codeLanguage)) {
            checkPythonCode(pythonImportList, blacklistGroupName, response, checkFlag);
        }
    }

    private void checkPythonCode(List<String> pythonImportList, String blacklistGroupName,
            Response response, boolean checkFlag) throws IOException {
        List<String> parseResult = pythonImportList;
        if (checkFlag) {
            parseResult = BlackListGroup.getSingleton().filterPythonImportInBlackList(
                    blacklistGroupName, pythonImportList);
        }
        String parseMessage = "python codecheck success";
        Map<String, Object> retJson = new HashMap<>();
        retJson.put("parse_status", "success");
        retJson.put("parse_result", parseResult);
        retJson.put("parse_message", parseMessage);
        writeData(true, parseMessage, HttpReturnCode.DEFAULT, retJson, response);
    }

    private void checkJavaCode(String parserGroupName, String decodedJavaCodeFrame, String blacklistGroupName,
            Response response, boolean checkFlag) throws IOException {
        String parseMessage = "";
        if (parserGroupName == null || parserGroupName.trim().isEmpty()) {
            parseMessage = "param parser_group_name should not be empty";
            writeData(false, parseMessage, HttpReturnCode.ERROR_PARAM_INVALID, response);
            return;
        }
        MethodParser methodParser = MethodParserGroup.getSingleton().getParserGroup(parserGroupName);
        if (methodParser == null) {
            parseMessage = "error parser_group_name(" + parserGroupName + ")";
            writeData(false, parseMessage, HttpReturnCode.ERROR_PARAM_INVALID, response);
            return;
        }
        List<MethodParserResult> parseResultList;
        try {
            parseResultList = methodParser.parseJavaCode(decodedJavaCodeFrame, true);
            if (checkFlag) {
                parseResultList = BlackListGroup.getSingleton()
                        .filterJavaMethodInBlackList(blacklistGroupName, parseResultList);
            }
            parseMessage = "java codecheck success";
            Map<String, Object> retJson = new HashMap<>();
            retJson.put("parse_status", "success");
            retJson.put("parse_result", parseResultList);
            retJson.put("parse_message", parseMessage);
            writeData(true, parseMessage, HttpReturnCode.DEFAULT, retJson, response);
        } catch (ParseProblemException ex) {
            //Problem stacktrace :
            parseMessage = ex.getProblems().get(0).getVerboseMessage();
            writeData(false, parseMessage, HttpReturnCode.ERROR_PARAM_INVALID, response);
        }
    }

    /**
     * doGet
     *
     * @param request
     * @param response
     */
    @Override
    public void doGet(Request request, Response response) throws Exception {
        doPost(request, response);
    }
}
