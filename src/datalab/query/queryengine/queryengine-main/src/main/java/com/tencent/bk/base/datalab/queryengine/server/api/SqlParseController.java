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

import static com.tencent.bk.base.datalab.queryengine.server.constant.RequestConstants.REQUEST_PARAMS_SQL;
import static com.tencent.bk.base.datalab.queryengine.server.util.BkSqlUtil.getResultTableIds;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.base.ApiResponse;
import com.tencent.bk.base.datalab.queryengine.server.service.SqlParseService;
import com.tencent.bk.base.datalab.queryengine.server.vo.QueryTaskVo;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import java.util.Map;
import java.util.Set;
import javax.validation.Valid;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 语法解析&语法校验 Controller
 */
@RateLimiter(name = "global")
@RestController
@RequestMapping(value = "/queryengine/sqlparse",
        produces = MediaType.APPLICATION_JSON_UTF8_VALUE,
        consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)

public class SqlParseController {

    public static final String AST = "ast";
    @Autowired
    SqlParseService sqlParseService;

    /**
     * sql 语法校验
     *
     * @param queryTaskVo 查询任务 vo
     * @return 校验结果
     * @throws Exception 校验异常
     */
    @PostMapping(value = "/syntax_check/")
    public ApiResponse<Object> syntaxCheck(@Valid @RequestBody QueryTaskVo queryTaskVo)
            throws Exception {
        String sql = queryTaskVo.getSql();
        String parseTree = sqlParseService.checkSyntax(sql);
        if (parseTree == null) {
            return ApiResponse.error();
        }
        return ApiResponse.success(ImmutableMap.of(AST, JacksonUtil.convertJson2Map(parseTree)));
    }

    /**
     * 提取结果表名
     *
     * @param body 请求 sql
     * @return 结果表名
     * @throws Exception 解析异常
     */
    @PostMapping(value = "/result_table/")
    public ApiResponse<Object> getResultTable(@RequestBody Map<String, String> body) {
        String sql = body.get(REQUEST_PARAMS_SQL);
        Preconditions.checkArgument(StringUtils.isNotBlank(sql),
                "sql can not be null or empty");
        Set<String> resultTables = getResultTableIds(sql);
        return ApiResponse.success(resultTables);
    }

    /**
     * 提取结果表名
     *
     * @param body 请求 sql
     * @return 结果表名
     * @throws Exception 解析异常
     */
    @PostMapping(value = "/sqltype_and_result_tables/")
    public ApiResponse<Object> getSqlTypeAndResultTables(@RequestBody Map<String, String> body)
            throws Exception {
        String sql = body.get(REQUEST_PARAMS_SQL);
        Preconditions.checkArgument(StringUtils.isNotBlank(sql),
                "sql can not be null or empty");
        Map<String, Object> resultMap = sqlParseService.getStatementTypeAndResultTables(sql, null);
        return ApiResponse.success(resultMap);
    }
}