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

import static com.tencent.bk.base.dataflow.codecheck.util.Constants.DEFAULT_SOURCE_DIR;

import com.tencent.bk.base.dataflow.codecheck.MethodParserGroup;
import com.tencent.bk.base.dataflow.codecheck.util.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.codecheck.util.Constants;
import com.tencent.bk.base.dataflow.codecheck.util.HttpReturnCode;
import com.tencent.bk.base.dataflow.codecheck.util.ParameterUtils;
import com.tencent.bk.base.dataflow.codecheck.util.db.CodeCheckDBUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class ParserGroupOpHandler extends AbstractHttpHandler {

    /**
     * doGet
     *
     * @param request
     * @param response
     */
    @Override
    public void doGet(Request request, Response response) throws Exception {
        ParameterUtils.parameterTransform(request);
        String parserGroupName = request.getParameter(Constants.PARSER_GROUP_NAME);
        String parserOpName = request.getParameter(Constants.PARSER_OP);
        String parserGroupMessage = "";
        if (parserOpName == null || parserOpName.trim().isEmpty()) {
            parserGroupMessage = "param parser_op should not be empty";
            writeData(false, parserGroupMessage, HttpReturnCode.ERROR_PARAM_INVALID, response);
            return;
        }
        Map<String, Object> retJson = new HashMap<>();
        retJson.put(Constants.PARSER_OP, parserOpName);
        retJson.put(Constants.PARSER_GROUP_NAME, parserGroupName);
        switch (parserOpName) {
            case Constants.PARSER_OP_ADD:
                addOp(retJson, parserGroupName, request, response);
                break;
            case Constants.PARSER_OP_DELETE:
                MethodParserGroup.getSingleton().deleteParserGroup(parserGroupName);
                // delete in db
                CodeCheckDBUtil.deleteParserGroup(parserGroupName);
                retJson.put("status", "success");
                writeData(true, parserGroupMessage, HttpReturnCode.DEFAULT, retJson, response);
                break;
            case Constants.PARSER_OP_LIST:
                retJson.put("status", "success");
                retJson.put("result", MethodParserGroup.getSingleton().listParserGroup(parserGroupName));
                writeData(true, parserGroupMessage, HttpReturnCode.DEFAULT, retJson, response);
                break;
            default:
                parserGroupMessage = "not supported op";
                retJson.put("status", "fail");
                retJson.put("message", parserGroupMessage);
                writeData(false, parserGroupMessage, HttpReturnCode.DEFAULT, response);
        }
    }

    private void addOp(Map retJson, String parserGroupName, Request request, Response response) throws IOException {
        String parserGroupMessage = "";
        retJson.put(Constants.PARSER_GROUP_NAME, parserGroupName);
        String libDirName = request.getParameter(Constants.LIB_DIR);
        if (libDirName == null || libDirName.trim().isEmpty()) {
            parserGroupMessage = "param lib_dir should not be empty";
            writeData(false, parserGroupMessage, HttpReturnCode.ERROR_PARAM_INVALID, response);
            return;
        }
        String sourceDirName = request.getParameter(Constants.SOURCE_DIR);
        if (sourceDirName == null || sourceDirName.trim().isEmpty()) {
            sourceDirName = DEFAULT_SOURCE_DIR;
        }
        try {
            MethodParserGroup.getSingleton().addParserGroup(parserGroupName, libDirName, sourceDirName);
            CodeCheckDBUtil.addParserGroup(parserGroupName, libDirName, sourceDirName);
            retJson.put(Constants.LIB_DIR, libDirName);
            retJson.put(Constants.SOURCE_DIR, sourceDirName);
            retJson.put("status", "success");
            writeData(true, parserGroupMessage, HttpReturnCode.DEFAULT, retJson, response);
        } catch (Exception ex) {
            String parserExMsg = ex.getMessage();
            retJson.put("status", "fail");
            retJson.put("message", parserExMsg);
            writeData(false, parserExMsg, HttpReturnCode.DEFAULT, response);
        }
    }

    /**
     * doPost
     *
     * @param request
     * @param response
     */
    @Override
    public void doPost(Request request, Response response) throws Exception {
        doGet(request, response);
    }
}
