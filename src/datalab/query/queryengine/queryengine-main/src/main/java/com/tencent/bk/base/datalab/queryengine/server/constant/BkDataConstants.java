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

package com.tencent.bk.base.datalab.queryengine.server.constant;

import com.tencent.bk.base.datalab.queryengine.server.configure.ApplicationConfig;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;

public class BkDataConstants {

    /**
     * 结果表操作类型：查询
     */
    public static final String PIZZA_AUTH_ACTION_QUERY_RT = "result_table.query_data";

    /**
     * 结果表操作类型：更新
     */
    public static final String PIZZA_AUTH_ACTION_UPDATE_RT = "result_table.update_data";

    /**
     * 结果表操作类型：删除
     */
    public static final String PIZZA_AUTH_ACTION_DELETE_RT = "result_table.delete_data";

    /**
     * 蓝鲸基础计算平台内部 AppCode
     */
    public static final String INNER_APP_CODE = SpringBeanUtil.getBean(ApplicationConfig.class)
            .getBkDataInnerAppCode();

    /**
     * 蓝鲸基础计算平台内部 AppSecret
     */
    public static final String INNER_APP_SECRET = SpringBeanUtil.getBean(ApplicationConfig.class)
            .getBkDataInnerAppSecret();

    /**
     * 蓝鲸基础计算平台语言环境设置
     */
    public static final String BLUEKING_LANGUAGE_KEY = "blueking-language";

    /**
     * 结果表默认地域标签
     */
    public static final String AREA_TAG_INLAND = "INLAND";

    /**
     * 权限校验方式
     */
    public static final String AUTH_METHOD_USERNAME = "user";
    public static final String AUTH_METHOD_APPCODE = "appcode";
    public static final String AUTH_METHOD_TOKEN = "token";

    /**
     * 结果表 processing 类型
     */
    public static final String PROCESSING_TYPE_BATCH = "batch";
    public static final String PROCESSING_TYPE_STREAM = "stream";
    public static final String PROCESSING_TYPE_QUERYSET = "queryset";
    public static final String PROCESSING_TYPE_SNAPSHOT = "snapshot";

    /**
     * 结果表 data_type
     */
    public static final String DATA_TYPE = "data_type";

    /**
     * 加解密相关 INSTANCE_KEY
     */
    public static final String BKDATA_INSTANCE_KEY = SpringBeanUtil.getBean(ApplicationConfig.class)
            .getBkDataInstanceKey();

    /**
     * 加解密相关 ROOT_KEY
     */
    public static final String BKDATA_ROOT_KEY = SpringBeanUtil.getBean(ApplicationConfig.class)
            .getBkDataRootKey();

    /**
     * 加解密相关 ROOT_IV
     */
    public static final String BKDATA_ROOT_IV = SpringBeanUtil.getBean(ApplicationConfig.class)
            .getBkDataRootIv();
}
