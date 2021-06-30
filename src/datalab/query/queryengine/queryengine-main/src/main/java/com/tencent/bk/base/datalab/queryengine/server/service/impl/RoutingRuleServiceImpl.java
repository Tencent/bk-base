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
import com.tencent.bk.base.datalab.queryengine.server.mapper.RoutingRuleMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.RoutingRule;
import com.tencent.bk.base.datalab.queryengine.server.service.RoutingRuleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RoutingRuleServiceImpl extends BaseModelServiceImpl<RoutingRule> implements
        RoutingRuleService {

    @Autowired
    private RoutingRuleMapper routingRuleMapper;

    public RoutingRuleServiceImpl() {
        this(CommonConstants.TB_DATAQUERY_ROUTING_RULE);
    }

    private RoutingRuleServiceImpl(String tableName) {
        super(tableName);
    }

    @Override
    public RoutingRule insert(RoutingRule routingRule) {
        Preconditions.checkArgument(routingRule != null, "routingRule can not be null");
        int rows = routingRuleMapper.insert(routingRule);
        if (rows > 0) {
            return routingRuleMapper.load(CommonConstants.TB_DATAQUERY_ROUTING_RULE, routingRule.getId());
        }
        return null;
    }

    @Override
    public RoutingRule update(RoutingRule routingRule) {
        Preconditions.checkArgument(routingRule != null, "routingRule can not be null");
        int rows = routingRuleMapper.update(routingRule);
        if (rows > 0) {
            return routingRuleMapper.load(CommonConstants.TB_DATAQUERY_ROUTING_RULE, routingRule.getId());
        }
        return null;
    }
}
