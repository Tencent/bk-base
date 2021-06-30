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

import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants;
import com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.service.SqlAdminService;
import com.tencent.bk.base.datalab.queryengine.server.third.DataLabApiService;
import com.tencent.bk.base.datalab.queryengine.server.third.NoteBookOutputDetail;
import com.tencent.bk.base.datalab.queryengine.server.third.NoteBookOutputs;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SqlAdminServiceImpl implements SqlAdminService {

    @Autowired
    private BKSqlService bkSqlService;

    @Override
    public void showResultTables(QueryTaskContext queryContext) throws Exception {
        DataLabApiService dataLabApiService = SpringBeanUtil.getBean(DataLabApiService.class);
        QueryTaskInfo qt = queryContext.getQueryTaskInfo();
        String noteBookId = (String) qt.getProperties().get(ThirdApiConstants.NOTEBOOK_ID);
        NoteBookOutputs outputs = dataLabApiService.showOutputs(noteBookId);
        queryContext.setNoteBookOutputs(outputs);
    }

    @Override
    public void showCreateTableSql(QueryTaskContext queryContext) throws Exception {
        DataLabApiService dataLabApiService = SpringBeanUtil.getBean(DataLabApiService.class);
        QueryTaskInfo qt = queryContext.getQueryTaskInfo();
        String noteBookId = (String) qt.getProperties().get(ThirdApiConstants.NOTEBOOK_ID);
        String resultTableId = (String) bkSqlService
                .convert(BkSqlContants.BKSQL_PROTOCOL_SHOWSQL_TABLE_NAME,
                        new ParsingContext(qt.getSqlText(), qt.getProperties()));
        NoteBookOutputDetail noteBookOutputDetail = dataLabApiService
                .showOutputDetail(noteBookId, resultTableId);
        queryContext.setNoteBookOutputDetail(noteBookOutputDetail);
    }
}
