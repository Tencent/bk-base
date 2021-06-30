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

import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.queryengine.server.base.ApiResponse;
import com.tencent.bk.base.datalab.queryengine.server.base.BaseController;
import com.tencent.bk.base.datalab.queryengine.server.model.ForbiddenConfig;
import com.tencent.bk.base.datalab.queryengine.server.service.ForbiddenConfigService;
import com.tencent.bk.base.datalab.queryengine.server.vo.ForbiddenConfigVo;
import com.tencent.bk.base.datalab.queryengine.server.vo.PageRequestVo;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import java.util.List;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 查询禁用 Controller
 */
@RateLimiter(name = "global")
@RestController
@RequestMapping(value = "/queryengine/forbidden_config", produces =
        MediaType.APPLICATION_JSON_UTF8_VALUE,
        consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
public class ForbiddenConfigController extends BaseController {

    @Autowired
    private ForbiddenConfigService queryForbiddenService;

    /**
     * 根据 id 来获取查询禁用规则列表
     *
     * @param id 查询禁用规则主键 id
     * @return ApiResponse
     */
    @GetMapping("/{id}")
    public ApiResponse<Object> load(@PathVariable int id) {
        ForbiddenConfig queryForbidden = queryForbiddenService.load(id);
        return ApiResponse.success(queryForbidden);
    }

    /**
     * 分页查询
     *
     * @param pageRequestVo 分页vo
     * @return ApiResponse
     */
    @PostMapping(value = "/pageList")
    public ApiResponse<Object> pageList(@Valid @RequestBody PageRequestVo pageRequestVo) {
        List<ForbiddenConfig> list = queryForbiddenService
                .pageList(pageRequestVo.getPage(), pageRequestVo.getPageSize());
        return ApiResponse.success(list);
    }

    /**
     * 获取返回的分页条数
     *
     * @param pageRequestVo 分页 vo
     * @return ApiResponse
     */
    @PostMapping(value = "/pageListCount")
    public ApiResponse<Object> pageListCount(@Valid @RequestBody PageRequestVo pageRequestVo) {
        int count = queryForbiddenService
                .pageListCount(pageRequestVo.getPage(), pageRequestVo.getPageSize());
        return ApiResponse.success(Maps.newHashMap()
                .put("count", count));
    }

    /**
     * 根据 id 来删除查询禁用规则
     *
     * @param id 查询禁用规则主键 id
     * @return ApiResponse
     */
    @DeleteMapping(value = "/{id}")
    public ApiResponse<Object> delete(@PathVariable Integer id) {
        int rows = queryForbiddenService.delete(id);
        if (rows == 1) {
            return ApiResponse.success();
        }
        return ApiResponse.error();
    }

    /**
     * 添加查询禁用规则
     *
     * @param queryForbiddenVo 查询禁用规则
     * @return ApiResponse
     */
    @PostMapping(value = "/")
    public ApiResponse<Object> insert(@Valid @RequestBody ForbiddenConfigVo queryForbiddenVo) {
        ForbiddenConfig queryForbidden = new ForbiddenConfig();
        queryForbidden.setResultTableId(queryForbiddenVo.getResultTableId());
        queryForbidden.setStorageType(queryForbiddenVo.getStorageType());
        queryForbidden.setStorageClusterConfigId(queryForbiddenVo.getStorageClusterConfigId());
        queryForbidden.setActive(1);
        queryForbidden.setUserName(queryForbiddenVo.getUserName());
        queryForbidden.setCreatedBy(queryForbiddenVo.getBkUserName());
        queryForbidden.setUpdatedBy(queryForbiddenVo.getBkUserName());
        queryForbidden.setDescription(queryForbiddenVo.getDescription());
        ForbiddenConfig result = queryForbiddenService.insert(queryForbidden);
        if (result != null) {
            return ApiResponse.success(result);
        }
        return ApiResponse.error();
    }

    /**
     * 更新查询禁用规则
     *
     * @param queryForbiddenVo 待更新的查询禁用规则
     * @return ApiResponse
     */
    @PutMapping(value = "/{id}")
    public ApiResponse<Object> update(@PathVariable Integer id,
            @Valid @RequestBody ForbiddenConfigVo queryForbiddenVo) {
        ForbiddenConfig queryForbidden = new ForbiddenConfig();
        queryForbidden.setResultTableId(queryForbiddenVo.getResultTableId());
        queryForbidden.setStorageType(queryForbiddenVo.getStorageType());
        queryForbidden.setStorageClusterConfigId(queryForbiddenVo.getStorageClusterConfigId());
        queryForbidden.setActive(queryForbiddenVo.getActive());
        queryForbidden.setUserName(queryForbiddenVo.getUserName());
        queryForbidden.setCreatedBy(queryForbiddenVo.getBkUserName());
        queryForbidden.setUpdatedBy(queryForbiddenVo.getBkUserName());
        queryForbidden.setDescription(queryForbiddenVo.getDescription());
        queryForbidden.setId(id);
        ForbiddenConfig result = queryForbiddenService.update(queryForbidden);
        if (result != null) {
            return ApiResponse.success(result);
        }
        return ApiResponse.error();
    }
}