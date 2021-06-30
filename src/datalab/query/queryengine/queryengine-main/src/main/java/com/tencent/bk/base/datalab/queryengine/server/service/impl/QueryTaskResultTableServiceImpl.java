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
import com.tencent.bk.base.datalab.queryengine.server.mapper.QueryTaskResultTableMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskResultTable;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskResultTableService;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class QueryTaskResultTableServiceImpl extends
        BaseModelServiceImpl<QueryTaskResultTable> implements QueryTaskResultTableService {

    @Autowired
    private QueryTaskResultTableMapper queryTaskResultTableMapper;

    public QueryTaskResultTableServiceImpl() {
        this(CommonConstants.TB_DATAQUERY_QUERYTASK_RESULT_TABLE);
    }

    private QueryTaskResultTableServiceImpl(String tableName) {
        super(tableName);
    }

    @Override
    public QueryTaskResultTable insert(QueryTaskResultTable queryTaskResultTable) {
        Preconditions.checkArgument(queryTaskResultTable != null,
                "queryTaskResultTable can not be null");
        int rows = queryTaskResultTableMapper.insert(queryTaskResultTable);
        if (rows > 0) {
            return queryTaskResultTableMapper
                    .load(CommonConstants.TB_DATAQUERY_QUERYTASK_RESULT_TABLE, queryTaskResultTable.getId());
        }
        return null;
    }

    @Override
    public QueryTaskResultTable update(QueryTaskResultTable queryTaskResultTable) {
        Preconditions.checkArgument(queryTaskResultTable != null,
                "queryTaskResultTable can not be null");
        int rows = queryTaskResultTableMapper.update(queryTaskResultTable);
        if (rows > 0) {
            return queryTaskResultTableMapper
                    .load(CommonConstants.TB_DATAQUERY_QUERYTASK_RESULT_TABLE, queryTaskResultTable.getId());
        }
        return null;
    }

    @Override
    public List<QueryTaskResultTable> loadByQueryId(String queryId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        return queryTaskResultTableMapper.loadByQueryId(queryId);
    }
}
