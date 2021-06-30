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
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskResultTable;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * 查询任务结果表 mapper
 */
@Mapper
public interface QueryTaskResultTableMapper extends BaseModelMapper<QueryTaskResultTable> {

    @Override
    @Insert("insert into dataquery_querytask_result_table (query_id,result_table_id,"
            + "storage_cluster_config_id,physical_table_name,cluster_name,cluster_type,"
            + "cluster_group,connection_info,priority,version,belongs_to,storage_config,"
            + "created_by,description) values (#{queryId},#{resultTableId},"
            + "#{storageClusterConfigId},#{physicalTableName},#{clusterName},#{clusterType},"
            + "#{clusterGroup},#{connectionInfo},#{priority},#{version},#{belongsTo},"
            + "#{storageConfig},#{createdBy},#{description})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(QueryTaskResultTable queryTaskResultTable);

    @Override
    @Update("update dataquery_querytask_result_table set query_id=#{queryId},"
            + "result_table_id=#{resultTableId},"
            + "storage_cluster_config_id=#{storageClusterConfigId},"
            + "physical_table_name=#{physicalTableName},cluster_name=#{clusterName},"
            + "cluster_type=#{clusterType},cluster_group=#{clusterGroup},"
            + "connection_info=#{connectionInfo},priority=#{priority},version=#{version},"
            + "belongs_to=#{belongsTo},storage_config=#{storageConfig},active=#{active},"
            + "updated_by=#{updatedBy} where id=#{id}")
    int update(QueryTaskResultTable queryTaskResultTable);

    /**
     * 根据queryId查询结果集元数据
     *
     * @param queryId 查询Id
     * @return 查询结果集元数据
     */
    @Select("select * from dataquery_querytask_result_table where query_id=#{queryId}")
    List<QueryTaskResultTable> loadByQueryId(String queryId);
}
