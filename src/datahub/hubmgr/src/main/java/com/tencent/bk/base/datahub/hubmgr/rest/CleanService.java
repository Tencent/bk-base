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

package com.tencent.bk.base.datahub.hubmgr.rest;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.CleanDebugParam;
import com.tencent.bk.base.datahub.hubmgr.utils.CleanDebugUtils;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 提供给databusAPI 清洗debug功能
 */
@Path("/clean")
public class CleanService {

    private static final Logger log = LoggerFactory.getLogger(CleanService.class);
    private static final String OK = "ok";
    private static final String ERROR = "error";

    /**
     * 验证清洗debug
     *
     * @param cleanDebugParam 清洗配置
     */
    @POST
    @Path("/verify")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult cleanVerify(final CleanDebugParam cleanDebugParam) {
        try {
            LogUtils.debug(log, "clean/verify conf:{} msg:{}", cleanDebugParam.getConf(), cleanDebugParam.getMsg());
            JSONObject confJson = JSONObject.fromObject(cleanDebugParam.getConf());
            String result = CleanDebugUtils.verify(confJson.toString(), cleanDebugParam.getMsg());
            return new ApiResult(Consts.NORMAL_RETCODE, OK, true, result, "");
        } catch (Exception e) {
            return new ApiResult(Consts.SERVER_ERROR_RETCODE, ERROR, false, "", e.getMessage());
        }
    }

}
