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
import com.tencent.bk.base.datalab.queryengine.server.model.RoutingRule;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Update;

/**
 * 查询路由 mapper
 */
@Mapper
public interface RoutingRuleMapper extends BaseModelMapper<RoutingRule> {

    @Override
    @Insert("insert into dataquery_routing_rule (routing_config,routing_type,created_by,"
            + "description) values (#{routingConfig},#{routingType},#{createdBy},#{description})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(RoutingRule queryTaskRouting);

    @Override
    @Update("update dataquery_routing_rule a set a.routing_config=#{routingConfig},a"
            + ".routing_type=#{routingType},a.description=#{description},a.active=#{active},a"
            + ".updated_by=#{updatedBy} where a.id=#{id}")
    int update(RoutingRule queryTaskRouting);
}
