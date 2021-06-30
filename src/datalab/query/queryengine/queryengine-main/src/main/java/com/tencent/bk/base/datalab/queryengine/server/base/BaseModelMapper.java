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
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface BaseModelMapper<T> {

    /**
     * 根据主键id查询
     *
     * @param tableName 表名
     * @param id 主键 id
     * @return 记录
     */
    @Select("select * from ${tableName} where id=#{id}")
    T load(@Param("tableName") String tableName, @Param("id") int id);

    /**
     * 根据主键id删除
     *
     * @param tableName 表名
     * @param id 主键 id
     * @return 记录
     */
    @Delete("delete from ${tableName} where id=#{id}")
    int delete(@Param("tableName") String tableName, @Param("id") Integer id);

    /**
     * 查询全部数据(不应该加载全部数据，待修改)
     *
     * @param tableName 表名
     * @return 记录列表
     */
    @Select("select * from ${tableName}")
    List<T> loadAll(@Param("tableName") String tableName);

    /**
     * 分页查询
     *
     * @param offSet 初始 offset
     * @param pageSize 每页记录数
     * @param tableName 表名
     * @return 记录列表
     */
    @Select("select * from ${tableName} LIMIT #{page}, #{pageSize}")
    List<T> pageList(@Param("tableName") String tableName, @Param("page") int offSet,
            @Param("pageSize") int pageSize);

    /**
     * 分页记录数
     *
     * @param offSet 初始 offset
     * @param pageSize 每页记录数
     * @param tableName 表名
     * @return 返回的记录数
     */
    @Select("select count(1) from ${tableName} LIMIT #{page}, #{pageSize}")
    int pageListCount(@Param("tableName") String tableName, @Param("page") int offSet,
            @Param("pageSize") int pageSize);

    /**
     * 插入记录
     *
     * @param t Model 对象
     * @return 受影响的条数
     */
    int insert(T t);

    /**
     * 更新记录
     *
     * @param t Model对象
     * @return 受影响的条数
     */
    int update(T t);

}
