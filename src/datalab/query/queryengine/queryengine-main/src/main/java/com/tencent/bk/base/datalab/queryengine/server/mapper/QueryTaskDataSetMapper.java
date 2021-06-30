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

package com.tencent.bk.base.datalab.queryengine.server.mapper;

import com.tencent.bk.base.datalab.queryengine.server.base.BaseModelMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskDataSet;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * 查询任务结果集 mapper
 */
@Mapper
public interface QueryTaskDataSetMapper extends BaseModelMapper<QueryTaskDataSet> {

    @Override
    @Insert("insert into dataquery_querytask_dataset(query_id,result_table_id,"
            + "result_table_generate_type,cluster_name,cluster_type,connection_info,schema_info,"
            + "total_records,created_by,description) values (#{queryId},"
            + "#{resultTableId},#{resultTableGenerateType},#{clusterName},#{clusterType},"
            + "#{connectionInfo},#{schemaInfo},#{totalRecords},#{createdBy},#{description})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(QueryTaskDataSet queryTaskDataSet);

    @Override
    @Update("update dataquery_querytask_dataset a set a.query_id=#{queryId},a"
            + ".cluster_name=#{clusterName},a.cluster_type=#{clusterType},a"
            + ".connection_info=#{connectionInfo},a.schema_info=#{schemaInfo},a"
            + ".total_records=#{totalRecords},a.description=#{description},a.active=#{active},a"
            + ".updated_by=#{updatedBy},a.result_table_id=#{resultTableId} where a.id=#{id}")
    int update(QueryTaskDataSet queryTaskDataSet);

    /**
     * 根据queryId查询结果集元数据
     *
     * @param queryId 查询Id
     * @return 查询结果集元数据
     */
    @Select("select * from dataquery_querytask_dataset where query_id=#{queryId}")
    QueryTaskDataSet loadByQueryId(String queryId);
}
