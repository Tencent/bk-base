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

package com.tencent.bk.base.datalab.queryengine.server.util;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface ModelUtil {

    @Delete("delete from ${tableName}")
    void clearTb(@Param("tableName") String tableName);

    @Select("select count(*) from ${tableName}")
    int countTb(@Param("tableName") String tableName);

    @Update("truncate table ${tableName}")
    void truncateTb(@Param("tableName") String tableName);

    @Insert("insert into dataquery_query_template(template_name,sql_text,created_by,description) "
            + "values ('测试模板01','select * from tab', 'zhangsan', 'test')")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    void addQueryTemplate();

    @Insert("insert into dataquery_querytask_info (sql_text,converted_sql_text,prefer_storage,"
            + "query_id,query_start_time,eval_result,routing_id,created_by,description) values "
            + "('select * from tab','select a,b,c from tab','mysql','bk10000000',"
            + "'2019-11-11 11:00:00','',1,'wangwu','')")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    void addQueryTaskInfo();

    @Insert("insert into dataquery_querytask_stage (query_id,stage_seq,stage_type,"
            + "stage_start_time,created_by,description) values ('bk200000','1','checkauth',"
            + "'2019-09-11 12:00:00','zhangsan','')")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    void addQueryState();

    @Insert("insert into dataquery_querytask_result_table (query_id,result_table_id,"
            + "storage_cluster_config_id,physical_table_name,cluster_name,cluster_type,"
            + "cluster_group,connection_info,priority,version,belongs_to,storage_config,"
            + "created_by,description) values ('bk2002','tab',"
            + "'1','tab','default','mysql',"
            + "'default','','0','5.7.1','default',"
            + "'','wangwu','')")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    void addQueryTaskResultTable();
}