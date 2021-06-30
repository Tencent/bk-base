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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;

public class BeanValidators {

    /**
     * 调用 JSR303 的 validate 方法, 验证失败时抛出 ConstraintViolationException.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void validateWithException(Validator validator, Object object, Class<?>... groups)
            throws ConstraintViolationException {
        Set constraintViolations = validator.validate(object, groups);
        if (!constraintViolations.isEmpty()) {
            throw new ConstraintViolationException(constraintViolations);
        }
    }


    /**
     * 辅助方法
     *
     * @param e e
     * @return List
     */
    public static List<String> extractMessage(ConstraintViolationException e) {
        return extractMessage(e.getConstraintViolations());
    }

    /**
     * 辅助方法
     *
     * @param constraintViolations ConstraintViolation
     * @return List
     */
    @SuppressWarnings("rawtypes")
    public static List<String> extractMessage(
            Set<? extends ConstraintViolation> constraintViolations) {
        List<String> errorMessages = Lists.newArrayList();
        for (ConstraintViolation violation : constraintViolations) {
            errorMessages.add(violation.getMessage());
        }
        return errorMessages;
    }

    /**
     * 辅助方法
     *
     * @param e e
     * @return Map
     */
    public static Map<String, String> extractPropertyAndMessage(ConstraintViolationException e) {
        return extractPropertyAndMessage(e.getConstraintViolations());
    }

    /**
     * extractPropertyAndMessage
     *
     * @param constraintViolations constraintViolations
     * @return Map
     */
    public static Map<String, String> extractPropertyAndMessage(
            Set<? extends ConstraintViolation> constraintViolations) {
        Map<String, String> errorMessages = Maps.newHashMap();
        for (ConstraintViolation violation : constraintViolations) {
            errorMessages.put(violation.getPropertyPath()
                    .toString(), violation.getMessage());
        }
        return errorMessages;
    }

    /**
     * extractPropertyAndMessageAsList
     *
     * @param e e
     * @return List
     */
    public static List<String> extractPropertyAndMessageAsList(ConstraintViolationException e) {
        return extractPropertyAndMessageAsList(e.getConstraintViolations(), " ");
    }

    /**
     * extractPropertyAndMessageAsList
     *
     * @param constraintViolations constraintViolations
     * @return List
     */
    public static List<String> extractPropertyAndMessageAsList(
            Set<? extends ConstraintViolation> constraintViolations) {
        return extractPropertyAndMessageAsList(constraintViolations, " ");
    }


    /**
     * extractPropertyAndMessageAsList
     *
     * @param e e
     * @param separator separator
     * @return List
     */
    public static List<String> extractPropertyAndMessageAsList(ConstraintViolationException e,
            String separator) {
        return extractPropertyAndMessageAsList(e.getConstraintViolations(), separator);
    }

    /**
     * extractPropertyAndMessageAsList
     *
     * @param constraintViolations ConstraintViolation
     * @param separator separator
     * @return List
     */
    @SuppressWarnings("rawtypes")
    public static List<String> extractPropertyAndMessageAsList(
            Set<? extends ConstraintViolation> constraintViolations,
            String separator) {
        List<String> errorMessages = Lists.newArrayList();
        for (ConstraintViolation violation : constraintViolations) {
            errorMessages.add(violation.getPropertyPath() + separator + violation.getMessage());
        }
        return errorMessages;
    }
}