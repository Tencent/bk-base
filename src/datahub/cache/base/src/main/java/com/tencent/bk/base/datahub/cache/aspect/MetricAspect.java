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

package com.tencent.bk.base.datahub.cache.aspect;

import com.tencent.bk.base.datahub.cache.aspect.annotation.Metric;
import com.tencent.bk.base.datahub.cache.aspect.annotation.Value;
import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.cache.aspect.annotation.Tag;
import com.tencent.bk.base.datahub.databus.commons.metric.BkdataMetric;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Aspect
public class MetricAspect {

    private static final Logger log = LoggerFactory.getLogger(MetricAspect.class);

    /**
     * 获取方法上的注解
     *
     * @param joinPoint 切入点
     * @return 注解
     */
    private static Metric getMethodAnn(JoinPoint joinPoint) {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        return method.getAnnotation(Metric.class);
    }

    /**
     * 获取参数上的注解
     *
     * @param joinPoint 切入点
     * @return 注解
     */
    private static Map<Object, Map<String, Object>> getParameterMetric(JoinPoint joinPoint) {
        MethodSignature methodSignature = (MethodSignature) joinPoint.getSignature();
        Method method = methodSignature.getMethod();
        Parameter[] parameters = method.getParameters();
        Object[] args = joinPoint.getArgs();
        AtomicInteger argsIndex = new AtomicInteger(0);

        Map<String, Object> tagMetric = new HashMap<>();
        Map<String, Object> valueMetric = new HashMap<>();
        Map<Object, Map<String, Object>> metric = new HashMap<>();

        Stream<Parameter> stream = Arrays.stream(parameters);
        stream.forEach(parameter -> {
            Annotation[] annArray = parameter.getDeclaredAnnotations();
            Stream<Annotation> annStream = Arrays.stream(annArray);
            annStream.forEach(ann -> {
                Object value = getParameterValue(args[argsIndex.get()]);
                if (ann instanceof Tag) {
                    tagMetric.put(((Tag) ann).name(), value);
                }
                if (ann instanceof Value) {
                    valueMetric.put(((Value) ann).name(), value);
                }
            });
            argsIndex.getAndIncrement();
        });

        metric.put(Value.class.getSimpleName(), valueMetric);
        metric.put(Tag.class.getSimpleName(), tagMetric);
        return metric;
    }

    /**
     * 获取参数值
     *
     * @param o 对象
     * @return 不同对象参数，作为不同打点数据
     */
    private static Object getParameterValue(Object o) {
        if (o instanceof String) {
            return o;
        } else {
            if (o instanceof Collection) {
                return ((Collection) o).size();
            } else {
                if (o instanceof Map) {
                    return ((Map) o).size();
                }
            }
        }

        return o;
    }

    /**
     * 获取tag字符串
     *
     * @param map tag 指标
     * @return 不同对象参数，作为不同打点数据
     */
    private static String getTagFormatStr(Map<String, Object> map) {
        StringBuffer tagStr = new StringBuffer(String.format("ip=%s,", Utils.getInnerIp()));

        if (map != null) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                tagStr.append(String.format("%s=%s,", entry.getKey(), entry.getValue()));
            }
        }
        return tagStr.substring(0, tagStr.lastIndexOf(","));
    }

    /**
     * 获取值格式化字符串
     *
     * @param map value 指标
     * @return 不同对象参数，作为不同打点数据
     */
    private static Map<String, Number> getValueFormatStr(Map<String, Object> map) {
        Map<String, Number> result = new HashMap<>();

        if (map == null) {
            return result;
        }

        try {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                result.put(entry.getKey(), (Number) entry.getValue());
            }
        } catch (Exception e) {
            LogUtils.warn(log, "failed to transform value type to Number !", e);
            throw e;
        }
        return result;
    }

    /**
     * 切入逻辑
     *
     * @param joinPoint 切入点
     * @return 返回代理结果
     * @throws Throwable 异常
     */
    @Around("execution (* com..*.*(..)) && (@within(com.tencent.bk.base.datahub.cache.aspect.annotation.Metric) ||"
            + " @annotation(com.tencent.bk.base.datahub.cache.aspect.annotation.Metric))")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {
        long start = System.currentTimeMillis();
        Object obj = null;
        Metric metricAnn = getMethodAnn(joinPoint);
        if (metricAnn == null) {
            return (Object) joinPoint.proceed();
        }

        String name = metricAnn.name();

        Map<Object, Map<String, Object>> argsMetric = getParameterMetric(joinPoint);

        try {
            obj = (Object) joinPoint.proceed();
            // 上报打点
            Map<String, Object> valueMetric = argsMetric.get(Value.class.getSimpleName());
            if (valueMetric != null) {
                valueMetric.put(CacheConsts.COST_TIME, System.currentTimeMillis() - start);
            }
            BkdataMetric.getInstance().reportTsdbStat(name, getTagFormatStr(argsMetric.get(Tag.class.getSimpleName())),
                    getValueFormatStr(valueMetric), CacheConsts.COST_TIME);
        } catch (Exception e) {
            LogUtils.warn(log, "failed to execute cache sdk!", e);
            // 上报事件
            String tags = getTagFormatStr(argsMetric.get(Tag.class.getSimpleName()));
            Map<String, Number> values = getValueFormatStr(argsMetric.get(Value.class.getSimpleName()));
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString();
            String msg = String.format("query cache is error, message: %s, client info: %s, values: %s",
                    stackTrace.length() > 1000 ? stackTrace.substring(0, 1000) : stackTrace, tags, values);
            BkdataMetric.getInstance().reportEvent(name, UUID.randomUUID().toString(), CacheConsts.EVENT_TYPE, "", msg);
            BkdataMetric.getInstance().reportErrorLog(name, UUID.randomUUID().toString(), "", stackTrace);
            throw e;
        }

        return obj;
    }
}