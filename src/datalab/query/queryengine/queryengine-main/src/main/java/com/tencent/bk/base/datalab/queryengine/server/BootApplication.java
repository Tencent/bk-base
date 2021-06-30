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

package com.tencent.bk.base.datalab.queryengine.server;

import com.tencent.bk.base.datalab.queryengine.server.configure.PrestoConfig;
import com.tencent.bk.base.datalab.queryengine.server.eval.QueryEvaluatorFactory;
import com.tencent.bk.base.datalab.queryengine.server.querydriver.QueryDriverFactory;
import com.tencent.bk.base.datalab.queryengine.server.util.FunctionFactory;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.TypeFactory;
import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.retry.annotation.EnableRetry;

@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = "com.tencent.bk.base.datalab.queryengine.server*")
@ServletComponentScan(basePackages = "com.tencent.bk.base.datalab.queryengine.server*")
@EnableRetry
public class BootApplication {

    /**
     * spring 启动入库
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        SpringApplication bootApp = new SpringApplication(BootApplication.class);
        bootApp.setBannerMode(Banner.Mode.CONSOLE);
        bootApp.run(args);
        initFunctionRep();
        initDataTypeRep();
        initQueryDriverRep();
        initQueryEvaluatorRep();
        handleLoggingResourceNotFound();
        log.info("QueryEngine-PrestoConfig: {}",
                SpringBeanUtil.getBean(PrestoConfig.class));
    }

    /**
     * sun.util.logging.resources.logging 加载失败处理逻辑
     */
    private static void handleLoggingResourceNotFound() {
        try {
            Class<?> cls = Class.forName("sun.util.logging.PlatformLogger");
            Field field = cls.getDeclaredField("loggingEnabled");
            field.setAccessible(true);
            field.set(null, Boolean.FALSE);
        } catch (Exception e) {
            log.error("Unable to set PlatformLogger loggingEnabled to False", e);
        }
    }

    /**
     * 初始化自定义函数库
     */
    private static void initFunctionRep() {
        FunctionFactory.initRepository();
    }

    /**
     * 初始化自定义类型库
     */
    private static void initDataTypeRep() {
        TypeFactory.initRepository();
    }

    /**
     * 初始化 QueryDriver
     */
    private static void initQueryDriverRep() {
        QueryDriverFactory.initDriverMap();
    }

    /**
     * 初始化 QueryEvaluator
     */
    private static void initQueryEvaluatorRep() {
        QueryEvaluatorFactory.initEvaluatorMap();
    }
}