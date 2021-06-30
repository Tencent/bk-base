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

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import com.tencent.bk.base.datahub.hubmgr.utils.DistributeLock;
import com.tencent.bk.base.datahub.hubmgr.utils.MgrNotifyUtils;
import java.lang.reflect.Method;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Aspect
public class JobAspect {

    private static final Logger log = LoggerFactory.getLogger(JobAspect.class);

    /**
     * 切入点执行逻辑
     *
     * @param joinPoint 切入点
     * @return 切点结果
     * @throws Throwable 异常
     */
    @Around("execution (* com..*.*(..)) && (@within(com.tencent.bk.base.datahub.hubmgr.job.aspect.JobService) "
            + "|| @annotation(com.tencent.bk.base.datahub.hubmgr.job.aspect.JobService))")
    public Object around(ProceedingJoinPoint joinPoint) throws Exception {
        long start = System.currentTimeMillis();
        JobService jobAnn = getJobAnn(joinPoint);
        if (jobAnn == null) {
            return executePoint(joinPoint);
        }

        String description = jobAnn.description();
        LogUtils.info(log, String.format("开始执行任务：%s， 触发时间：%s", description, start));

        String lockPath = jobAnn.lockPath();
        String receivers = DatabusProps.getInstance().getOrDefault(MgrConsts.DATABUS_ADMIN, "");

        if (StringUtils.isNotBlank(lockPath)) {
            int lease = jobAnn.lease();
            try (DistributeLock distributeLock = new DistributeLock(String.format("/databusmgr/lock/%s", lockPath))) {
                if (distributeLock.lock(lease)) {
                    Object obj = executePoint(joinPoint);
                    // 确保占用锁的时间超过LOCK_SEC的时间，避免释放锁太快导致其他进程里的job获取到锁，重复执行
                    long duration = System.currentTimeMillis() - start;
                    if (duration < (10 + lease) * 1000L) {
                        try {
                            Thread.sleep((10 + lease) * 1000L - duration);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    return obj;
                } else {
                    // 获取执行锁失败
                    LogUtils.info(log, "unable to get a lock to execute job logic!");
                }
            } catch (Exception e) {
                LogUtils.warn(log, "failed to run databus cluster status check job!", e);
                // 集群状态检查失败时，需要通知管理员
                MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                        String.format("执行%s失败,原因：%s", description, ExceptionUtils.getStackTrace(e)));
                throw new JobExecutionException(e);
            }
        }
        return null;
    }

    /**
     * 获取job注解
     *
     * @param joinPoint 切入点
     */
    private static JobService getJobAnn(JoinPoint joinPoint) {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        return method.getAnnotation(JobService.class);
    }

    /**
     * 执行joinPoint
     */
    private static Object executePoint(ProceedingJoinPoint joinPoint) {
        try {
            return (Object) joinPoint.proceed();
        } catch (Throwable th) {
            LogUtils.warn(log, String.format("执行job任务异常：%s", th.getMessage()));
            throw new RuntimeException(th.getMessage());
        }
    }
}
