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
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * 查询任务 mapper
 */
@Mapper
public interface QueryTaskInfoMapper extends BaseModelMapper<QueryTaskInfo> {

    @Override
    @Insert("insert into dataquery_querytask_info (sql_text,converted_sql_text,prefer_storage,"
            + "query_id,"
            + "query_start_time,eval_result,routing_id,created_by,description) values "
            + "(#{sqlText},#{convertedSqlText},#{preferStorage},#{queryId},#{queryStartTime},"
            + "#{evalResult},"
            + "#{routingId},#{createdBy},#{description})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(QueryTaskInfo queryTaskInfo);

    @Override
    @Update("update dataquery_querytask_info set sql_text=#{sqlText},"
            + "converted_sql_text=#{convertedSqlText},"
            + "prefer_storage=#{preferStorage},query_id=#{queryId},"
            + "query_start_time=#{queryStartTime},query_end_time=#{queryEndTime},"
            + "cost_time=#{costTime},total_records=#{totalRecords},query_state=#{queryState},"
            + "eval_result=#{evalResult},routing_id=#{routingId},description=#{description},"
            + "active=#{active},updated_by=#{updatedBy} where id=#{id}")
    int update(QueryTaskInfo queryTaskInfo);

    @Select("select * from ${tableName} where query_id=#{queryId}")
    QueryTaskInfo loadByQueryId(@Param("tableName") String tableName,
            @Param("queryId") String queryId);

    @Select({"<script>"
            + "select * from ${tableName} where query_id in"
            + "<foreach item='item' index='index' collection='queryIdList' open='(' separator=','"
            + " close=')'>"
            + "#{item}"
            + "</foreach>"
            + "order by updated_at desc"
            + "</script>"})
    List<QueryTaskInfo> loadByQueryIdList(@Param("tableName") String tableName,
            @Param("queryIdList") List<String> queryIdList);

    @Select({"<script>"
            + "select * from ${tableName} where query_id in"
            + "<foreach item='item' index='index' collection='queryIdList' open='(' separator=','"
            + " close=')'>"
            + "#{item}"
            + "</foreach>"
            + "order by updated_at desc "
            + "limit #{offSet}, #{pageSize}"
            + "</script>"})
    List<QueryTaskInfo> infoPageList(@Param("tableName") String tableName,
            @Param("queryIdList") List<String> queryIdList,
            @Param("offSet") int offSet,
            @Param("pageSize") int pageSize);

    @Select({"<script>"
            + "select count(1) from ${tableName} where query_id in"
            + "<foreach item='item' index='index' collection='queryIdList' open='(' separator=','"
            + " close=')'>"
            + "#{item}"
            + "</foreach>"
            + "</script>"})
    int infoPageListCount(@Param("tableName") String tableName,
            @Param("queryIdList") List<String> queryIdList);

    @Delete("delete from ${tableName} where query_id=#{queryId}")
    int deleteByQueryId(@Param("tableName") String tableName, @Param("queryId") String queryId);

    @Select("select * from ${tableName} where UNIX_TIMESTAMP(created_at) < UNIX_TIMESTAMP"
            + "(date_sub(curdate(),interval ${day} day)) and active=1")
    List<QueryTaskInfo> loadByNdaysAgo(@Param("tableName") String tableNameo,
            @Param("day") int day);
}
