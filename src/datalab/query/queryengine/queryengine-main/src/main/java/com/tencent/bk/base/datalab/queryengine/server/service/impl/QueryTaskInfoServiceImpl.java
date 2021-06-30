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

package com.tencent.bk.base.datalab.queryengine.server.service.impl;

import com.google.common.base.Preconditions;
import com.tencent.bk.base.datalab.queryengine.server.base.BaseModelServiceImpl;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.mapper.QueryTaskInfoMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskInfoService;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueryTaskInfoServiceImpl extends BaseModelServiceImpl<QueryTaskInfo> implements
        QueryTaskInfoService {

    @Autowired
    private QueryTaskInfoMapper queryTaskInfoMapper;

    public QueryTaskInfoServiceImpl() {
        this(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO);
    }

    private QueryTaskInfoServiceImpl(String tableName) {
        super(tableName);
    }

    @Override
    public QueryTaskInfo insert(QueryTaskInfo queryTaskInfo) {
        Preconditions.checkArgument(queryTaskInfo != null, "queryTaskInfo can not be null");
        int rows = queryTaskInfoMapper.insert(queryTaskInfo);
        if (rows > 0) {
            return queryTaskInfoMapper.load(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO, queryTaskInfo.getId());
        }
        return null;
    }

    @Override
    public QueryTaskInfo update(QueryTaskInfo queryTaskInfo) {
        Preconditions.checkArgument(queryTaskInfo != null, "queryTaskInfo can not be null");
        int rows = queryTaskInfoMapper.update(queryTaskInfo);
        if (rows > 0) {
            return queryTaskInfoMapper.load(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO, queryTaskInfo.getId());
        }
        return null;
    }

    @Override
    public QueryTaskInfo loadByQueryId(String queryId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        return queryTaskInfoMapper.loadByQueryId(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO, queryId);
    }

    @Override
    public List<QueryTaskInfo> loadByQueryIds(List<String> queryIdList) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(queryIdList),
                "queryIdList can not be null or empty");
        return queryTaskInfoMapper.loadByQueryIdList(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO, queryIdList);
    }

    @Override
    public List<QueryTaskInfo> loadByQueryIds(List<String> queryIdList, int page, int pageSize) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(queryIdList),
                "queryIdList can not be null or empty");
        Preconditions.checkArgument(page > 0, "page must be a positive number");
        Preconditions.checkArgument(pageSize > 0, "pageSize must be a positive number");
        int offSet = (page - 1) * pageSize;
        return queryTaskInfoMapper
                .infoPageList(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO, queryIdList, offSet, pageSize);
    }

    @Override
    public int infoPageListCount(List<String> queryIdList) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(queryIdList),
                "queryIdList can not be null or empty");
        return queryTaskInfoMapper.infoPageListCount(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO, queryIdList);
    }

    @Override
    public int deleteByQueryId(String queryId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        return queryTaskInfoMapper.deleteByQueryId(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO, queryId);
    }

    @Override
    public List<QueryTaskInfo> loadByNdaysAgo(int day) {
        return queryTaskInfoMapper.loadByNdaysAgo(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO, day);
    }
}
