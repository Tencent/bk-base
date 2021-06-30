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

package com.tencent.bk.base.datalab.queryengine.server.api;

import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.DATETIME_FORMAT;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.QUERYID_PREFIX;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.SYNC;
import static com.tencent.bk.base.datalab.queryengine.server.util.QueryTaskContextUtil.genSyncResponse;

import com.tencent.bk.base.datalab.queryengine.common.numeric.SimpleIdGenerator;
import com.tencent.bk.base.datalab.queryengine.common.time.DateUtil;
import com.tencent.bk.base.datalab.queryengine.server.base.ApiResponse;
import com.tencent.bk.base.datalab.queryengine.server.base.BaseController;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContext;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.service.QuerySyncService;
import com.tencent.bk.base.datalab.queryengine.server.vo.QueryTaskVo;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 同步查询 Controller
 */
@RateLimiter(name = "global")
@Slf4j
@RestController
@RequestMapping(value = {"/queryengine/query_sync",
        "/dataquery/query"}, produces = MediaType.APPLICATION_JSON_UTF8_VALUE,
        consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
public class QueryTaskSyncController extends BaseController {

    @Autowired
    SimpleIdGenerator simpleIdGenerator;

    @Autowired
    QuerySyncService querySyncService;

    /**
     * 提交同步 SQL 作业
     *
     * @param queryTaskVo 查询请求体
     * @return 查询结果
     */
    @PostMapping(value = "/")
    public ApiResponse<Object> createQuery(@RequestBody @Valid QueryTaskVo queryTaskVo)
            throws Exception {
        String stageStartTime = DateUtil.getCurrentDate(DATETIME_FORMAT);
        QueryTaskInfo queryTaskInfo = new QueryTaskInfo();
        queryTaskInfo.setPreferStorage(queryTaskVo.getPreferStorage());
        queryTaskInfo.setSqlText(queryTaskVo.getSql());
        queryTaskInfo.setQueryId(QUERYID_PREFIX + simpleIdGenerator.nextId());
        queryTaskInfo.setProperties(queryTaskVo.getProperties());
        queryTaskInfo.setQueryStartTime(stageStartTime);
        queryTaskInfo.setQueryMethod(SYNC);
        queryTaskInfo.setDescription(SYNC);
        BkAuthContext authContext = BkAuthContextHolder.get();
        if (StringUtils.isNotEmpty(authContext.getBkUserName())) {
            queryTaskInfo.setCreatedBy(authContext.getBkUserName());
        } else {
            queryTaskInfo.setCreatedBy(authContext.getBkAppCode());
        }
        QueryTaskContext queryTaskContext = new QueryTaskContext();
        queryTaskContext.setPreferStorage(queryTaskVo.getPreferStorage());
        queryTaskContext.setQueryTaskInfo(queryTaskInfo);
        queryTaskContext.setQueryTaskStageMap(new LinkedHashMap<>(16));
        querySyncService.query(queryTaskContext);
        Map<String, Object> fillResult = genSyncResponse(queryTaskContext);
        return ApiResponse.success(fillResult);
    }
}
