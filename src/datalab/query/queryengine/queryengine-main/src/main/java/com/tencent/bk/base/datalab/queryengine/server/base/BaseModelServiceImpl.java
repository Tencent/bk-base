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


import com.google.common.base.Preconditions;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class BaseModelServiceImpl<T> implements ModelService<T> {

    @Autowired
    private BaseModelMapper<T> baseModelMapper;

    private String tbName;

    public BaseModelServiceImpl() {
    }

    public BaseModelServiceImpl(String tbName) {
        this.tbName = tbName;
    }

    @Override
    public T load(int id) {
        Preconditions.checkArgument(id > 0, "Record id must be a positive number");
        return baseModelMapper.load(tbName, id);
    }

    @Override
    public List<T> loadAll() {
        return baseModelMapper.loadAll(tbName);
    }

    @Override
    public List<T> pageList(int page, int pageSize) {
        Preconditions.checkArgument(page > 0, "page must be a positive number");
        Preconditions.checkArgument(pageSize > 0, "pageSize must be a positive number");
        int offSet = (page - 1) * pageSize;
        return baseModelMapper.pageList(tbName, offSet, pageSize);
    }

    @Override
    public int pageListCount(int page, int pageSize) {
        Preconditions.checkArgument(page > 0, "page must be a positive number");
        Preconditions.checkArgument(pageSize > 0, "pageSize must be a positive number");
        int offSet = (page - 1) * pageSize;
        return baseModelMapper.pageListCount(tbName, offSet, pageSize);
    }

    @Override
    @Transactional(readOnly = false)
    public int delete(Integer id) {
        Preconditions.checkArgument(id > 0, "Record id must be a positive number");
        return baseModelMapper.delete(tbName, id);
    }
}
