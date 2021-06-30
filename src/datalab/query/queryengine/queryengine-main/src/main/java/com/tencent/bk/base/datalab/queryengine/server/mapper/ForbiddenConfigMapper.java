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
import com.tencent.bk.base.datalab.queryengine.server.model.ForbiddenConfig;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Update;

/**
 * 查询禁用规则 mapper
 */
@Mapper
public interface ForbiddenConfigMapper extends BaseModelMapper<ForbiddenConfig> {

    @Override
    @Insert("insert into dataquery_forbidden_config (result_table_id,user_name, storage_type,"
            + "storage_cluster_config_id,created_by,description) values (#{resultTableId},"
            + "#{userName},#{storageType},#{storageClusterConfigId},#{createdBy},#{description})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(ForbiddenConfig queryForbidden);

    @Override
    @Update("update dataquery_forbidden_config a set a.result_table_id=#{resultTableId},a"
            + ".user_name=#{userName},a.storage_type=#{storageType},a"
            + ".storage_cluster_config_id=#{storageClusterConfigId},a.description=#{description},"
            + "a.active=#{active},a.updated_by=#{updatedBy} where a.id=#{id}")
    int update(ForbiddenConfig queryForbidden);

}
