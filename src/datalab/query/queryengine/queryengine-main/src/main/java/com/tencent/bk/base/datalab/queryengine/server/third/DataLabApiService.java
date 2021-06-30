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

package com.tencent.bk.base.datalab.queryengine.server.third;

import com.tencent.bk.base.datalab.queryengine.server.dto.ResultTableProperty;

/**
 * datalab 第三方接口
 */
public interface DataLabApiService {

    /**
     * 查询结果表注册
     *
     * @param noteBookId 笔记 Id
     * @param cellId 笔记单元 Id
     * @param resultTable 结果表 Id
     * @return 是否注册成功 true:成功，false:失败
     */
    boolean registerResultTable(String noteBookId, String cellId, ResultTableProperty resultTable);

    /**
     * 获取笔记结果表产出物
     *
     * @param noteBookId 笔记 notebook_id
     * @return NoteBookOutputs 实例
     */
    NoteBookOutputs showOutputs(String noteBookId);

    /**
     * 获取笔记结果表详情
     *
     * @param noteBookId 笔记 notebook_id
     * @param resultTableId 结果表 Id
     * @return 结果表详细
     */
    NoteBookOutputDetail showOutputDetail(String noteBookId, String resultTableId);
}
