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

package com.tencent.bk.base.datahub.hubmgr.job.aspect;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.lang.reflect.Method;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Aspect
public class RetryAspect {

    private static final Logger log = LoggerFactory.getLogger(RetryAspect.class);

    /**
     * 切入点执行逻辑
     *
     * @param joinPoint 切入点
     * @return 切点结果
     * @throws Exception 异常
     */
    @Around("execution (* com..*.*(..)) && (@within(com.tencent.bk.base.datahub.hubmgr.job.aspect.Retry) "
            + "|| @annotation(com.tencent.bk.base.datahub.hubmgr.job.aspect.Retry))")
    public Object around(ProceedingJoinPoint joinPoint) throws Exception {
        Retry jobAnn = getJobAnn(joinPoint);
        if (jobAnn == null) {
            return executePoint(joinPoint);
        }

        return new RetryProcessor() {
            protected Object process() {
                return executePoint(joinPoint);
            }
        }.setRetryTime(jobAnn.times()).setInterval(jobAnn.interval()).execute();
    }

    /**
     * 获取job注解
     */
    private static Retry getJobAnn(JoinPoint joinPoint) {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        return method.getAnnotation(Retry.class);
    }

    /**
     * 执行joinPoint
     */
    private static Object executePoint(ProceedingJoinPoint joinPoint) {
        try {
            return (Object) joinPoint.proceed();
        } catch (Throwable th) {
            LogUtils.warn(log, String.format("execute point exception: %s", th.getMessage()));
            throw new RuntimeException(th.getMessage());
        }
    }
}

