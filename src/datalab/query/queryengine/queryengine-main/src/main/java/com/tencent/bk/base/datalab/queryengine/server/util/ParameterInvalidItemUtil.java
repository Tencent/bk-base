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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Set;
import javax.validation.ConstraintViolation;
import org.springframework.util.CollectionUtils;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;

/**
 * 参数无效项辅助器
 */
public class ParameterInvalidItemUtil {

    /**
     * 将 ConstraintViolation 的 Set 集合转换成 ParameterInvalidItem 列表
     *
     * @param cvSet ConstraintViolation 集合
     * @return 无效项列表
     */
    public static List<ParameterInvalidItem> convertCvSet(
            Set<ConstraintViolation<?>> cvSet) {
        if (CollectionUtils.isEmpty(cvSet)) {
            return null;
        }
        List<ParameterInvalidItem> parameterInvalidItemList = Lists.newArrayList();
        for (ConstraintViolation<?> cv : cvSet) {
            ParameterInvalidItem parameterInvalidItem = new ParameterInvalidItem();
            String propertyPath = cv.getPropertyPath()
                    .toString();
            if (propertyPath.indexOf(".") != -1) {
                String[] propertyPathArr = propertyPath.split("\\.");
                parameterInvalidItem.setFieldName(propertyPathArr[propertyPathArr.length - 1]);
            } else {
                parameterInvalidItem.setFieldName(propertyPath);
            }
            parameterInvalidItem.setMessage(cv.getMessage());
            parameterInvalidItemList.add(parameterInvalidItem);
        }
        return parameterInvalidItemList;
    }

    /**
     * 将 BindingResult 校验结果转换成无效项列表
     *
     * @param bindingResult 校验结果
     * @return 无效项列表
     */
    public static List<ParameterInvalidItem> convertBindingResult(
            BindingResult bindingResult) {
        if (bindingResult == null) {
            return null;
        }
        List<ParameterInvalidItem> parameterInvalidItemList = Lists.newArrayList();
        List<FieldError> fieldErrorList = bindingResult.getFieldErrors();
        for (FieldError fieldError : fieldErrorList) {
            ParameterInvalidItem parameterInvalidItem = new ParameterInvalidItem();
            parameterInvalidItem.setFieldName(fieldError.getField());
            parameterInvalidItem.setMessage(fieldError.getDefaultMessage());
            parameterInvalidItemList.add(parameterInvalidItem);
        }
        return parameterInvalidItemList;
    }
}
