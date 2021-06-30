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

import com.tencent.bk.base.dataflow.codecheck.util.BlackListGroup;
import com.tencent.bk.base.dataflow.codecheck.util.Constants;
import com.tencent.bk.base.dataflow.codecheck.util.HttpReturnCode;
import com.tencent.bk.base.dataflow.codecheck.util.db.CodeCheckDBUtil;
import com.tencent.bk.base.dataflow.codecheck.util.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.codecheck.util.ParameterUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class BlacklistGroupOpHandler extends AbstractHttpHandler {

    /**
     * doGet
     *
     * @param request
     * @param response
     */
    @Override
    public void doGet(Request request, Response response) throws Exception {
        ParameterUtils.parameterTransform(request);
        String blacklistGroupName = request.getParameter(Constants.BLACKLIST_GROUP_NAME);
        String blacklistOpName = request.getParameter(Constants.BLACKLIST_OP);
        String blacklistGroupMessage = "";
        if (blacklistOpName == null || blacklistOpName.trim().isEmpty()) {
            blacklistGroupMessage = "param blacklist_op should not be empty";
            writeData(false, blacklistGroupMessage, HttpReturnCode.ERROR_PARAM_INVALID, response);
            return;
        }
        Map<String, Object> retJson = new HashMap<>();
        retJson.put(Constants.BLACKLIST_OP, blacklistOpName);
        retJson.put(Constants.BLACKLIST_GROUP_NAME, blacklistGroupName);

        switch (blacklistOpName) {
            case Constants.BLACKLIST_OP_ADD:
                String blacklistStr = request.getParameter(Constants.BLACKLIST);
                if (blacklistStr == null || blacklistStr.trim().isEmpty()) {
                    blacklistStr = "";
                }
                retJson.put(Constants.BLACKLIST, blacklistStr);
                try {
                    BlackListGroup.getSingleton().addBlackListGroup(blacklistGroupName, blacklistStr);
                    // add in db
                    CodeCheckDBUtil.addBlacklistGroup(blacklistGroupName, blacklistStr);
                    retJson.put("status", "success");
                    writeData(true, blacklistGroupMessage, HttpReturnCode.DEFAULT, retJson, response);
                } catch (Exception ex) {
                    blacklistGroupMessage = ex.getMessage();
                    retJson.put("status", "fail");
                    retJson.put("message", blacklistGroupMessage);
                    writeData(false, blacklistGroupMessage, HttpReturnCode.DEFAULT, response);
                }
                break;
            case Constants.BLACKLIST_OP_DELETE:
                BlackListGroup.getSingleton().deleteBlackListGroup(blacklistGroupName);
                CodeCheckDBUtil.deleteBlacklistGroup(blacklistGroupName);
                retJson.put("status", "success");
                writeData(true, blacklistGroupMessage, HttpReturnCode.DEFAULT, retJson, response);
                break;
            case Constants.BLACKLIST_OP_LIST:
                Map<String, List<String>> blackListGroupInfo =
                        BlackListGroup.getSingleton().listBlackListGroup(blacklistGroupName);
                retJson.put("result", blackListGroupInfo);
                retJson.put("status", "success");
                writeData(true, blacklistGroupMessage, HttpReturnCode.DEFAULT, retJson, response);
                break;
            default:
                blacklistGroupMessage = "not supported op";
                retJson.put("status", "fail");
                retJson.put("message", blacklistGroupMessage);
                writeData(false, blacklistGroupMessage, HttpReturnCode.DEFAULT, response);
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
