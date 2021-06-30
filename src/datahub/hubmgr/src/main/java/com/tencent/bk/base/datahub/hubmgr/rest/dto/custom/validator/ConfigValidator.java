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

package com.tencent.bk.base.datahub.hubmgr.rest.dto.custom.validator;

import static com.tencent.bk.base.datahub.cache.ignite.IgUtils.ALLOW_REGIONS;
import static com.tencent.bk.base.datahub.cache.ignite.IgUtils.NAME_PATTERN;

import com.tencent.bk.base.datahub.hubmgr.rest.dto.custom.annotation.ConfigType;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.custom.annotation.ConfigValidate;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;


public class ConfigValidator implements ConstraintValidator<ConfigValidate, String> {

    private ConfigType type = ConfigType.CACHE_NAME;

    public void initialize(ConfigValidate cv) {
        this.type = cv.value();
    }

    /**
     * 是否有效
     *
     * @param value 校验的值
     * @param context 上下文
     * @return 是否校验成功
     */
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (type.getName().equals(ConfigType.CACHE_NAME.getName())
                || type.getName().equals(ConfigType.DB_SCHEMA.getName())) {
            return NAME_PATTERN.matcher(value).matches();
        }
        if (type.getName().equals(ConfigType.DATA_REGION.getName())) {
            return ALLOW_REGIONS.contains(value);
        }
        return false;
    }
}
