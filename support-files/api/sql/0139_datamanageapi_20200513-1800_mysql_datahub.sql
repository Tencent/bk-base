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

SET NAMES utf8;

USE bkdata_basic;


INSERT INTO content_language_config (
    `content_key`, `language`, `content_value`, `description`
) VALUES (
    '前序任务失败',
    'en',
    'Disabled',
    ''
);

INSERT INTO content_language_config (
    `content_key`, `language`, `content_value`, `description`
) VALUES (
    '前序任务执行失败',
    'en',
    'Parent task failed.',
    ''
);

INSERT INTO content_language_config (
    `content_key`, `language`, `content_value`, `description`
) VALUES (
    '任务失败',
    'en',
    'Failed',
    ''
);

INSERT INTO content_language_config (
    `content_key`, `language`, `content_value`, `description`
) VALUES (
    '任务执行失败',
    'en',
    'Task failed.',
    ''
);

INSERT INTO content_language_config (
    `content_key`, `language`, `content_value`, `description`
) VALUES (
    '任务无输出',
    'en',
    'Failed Succeeded',
    ''
);

INSERT INTO content_language_config (
    `content_key`, `language`, `content_value`, `description`
) VALUES (
    '任务无输出，前序任务没有数据或当前任务计算结果无数据',
    'en',
    'No output from this task. No data read from parent tasks or calculation result of the current task is empty.',
    ''
);

INSERT INTO content_language_config (
    `content_key`, `language`, `content_value`, `description`
) VALUES (
    '前序任务不满足执行条件',
    'en',
    'Skipped',
    ''
);

INSERT INTO content_language_config (
    `content_key`, `language`, `content_value`, `description`
) VALUES (
    '前序任务不满足执行条件，请等下一个周期',
    'en',
    'Parent tasks does not meet the execution conditions, please wait for the next cycle.',
    ''
);
