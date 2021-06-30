/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/* eslint-disable */
const scriptContent = {
  'zh-cn': `
    ## 本文件为Shell脚本，请使用Shell格式完成数据上报
    ## 请使用脚本对指标和维度赋值

    metric1=123 # metric1, 指标
    dimension1=abc # dimension1, 维度
    ## time=123 # 自定义时间，默认使用上报时间

    ## 将上述变量以 "Key Value"方式作为预置命令 （INSERT_METRIC,INSERT_DIMENSION）的参数以上报数据
    ## 上报参数说明 : INSERT_METRIC 指标字段名 指标字段值 ;  INSERT_DIMENSION 维度字段名 维度字段值 ; INSERT_TIMESTAMP 时间戳 ; COMMIT 提交上报数据

    INSERT_METRIC metric1 \${metric1}
    INSERT_DIMENSION dimension1 \${dimension1}
    ## INSERT_TIMESTAMP \${time}
    COMMIT

    ## 你可以开始在这里编写程序了`,

  en: `
    ## This file is a shell script, please use the Shell format to complete the data report.
    ## Please use scripts to assign values ​​to indicators and dimensions

    Metric1=123 # metric1, indicator
    Dimension1=abc # dimension1, dimension
    ## time=123 #Custom time, default report time

    ## Use the "Key Value" method as the parameter of the preset command (INSERT_METRIC, INSERT_DIMENSION) to report the data above.
    ## Reporting parameter description: INSERT_METRIC indicator field name indicator field value; INSERT_DIMENSION dimension field name dimension field value; INSERT_TIMESTAMP timestamp; COMMIT submit report data

    INSERT_METRIC metric1 \${metric1}
    INSERT_DIMENSION dimension1 \${dimension1}
    ## INSERT_TIMESTAMP \${time}
    COMMIT

    ## You can start writing programs here.`,
};

export { scriptContent };
