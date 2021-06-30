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

package com.tencent.bk.base.datalab.queryengine.server.base;

import java.util.List;

public interface ModelService<T> {

    /**
     * 根据 id 来获取记录
     *
     * @param id 主键id
     * @return 单个记录
     */
    T load(int id);

    /**
     * 获取记录列表
     *
     * @return 记录列表
     */
    List<T> loadAll();

    /**
     * 分页查询
     *
     * @param page 当前页
     * @param pageSize 每页记录数
     * @return 记录列表
     */
    List<T> pageList(int page, int pageSize);

    /**
     * 分页记录数
     *
     * @param page 当前页
     * @param pageSize 每页记录数
     * @return 返回的记录数
     */
    int pageListCount(int page, int pageSize);

    /**
     * 根据 id 来删除记录
     *
     * @param id 主键id
     * @return 影响的记录数
     */
    int delete(Integer id);

    /**
     * 新增记录
     *
     * @param t 新增的记录
     * @return 新增记录
     */
    default T insert(T t) {
        return t;
    }

    /**
     * 更新记录
     *
     * @param t 待更新的记录
     * @return 更新后的记录
     */
    default T update(T t) {
        return t;
    }
}
