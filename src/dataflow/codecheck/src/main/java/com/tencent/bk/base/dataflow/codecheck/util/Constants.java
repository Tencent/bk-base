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

package com.tencent.bk.base.dataflow.codecheck.util;

public class Constants {

    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String RELOAD_CONFIG_SEC = "reload_config_sec";

    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 6384;
    public static final int DEFAULT_RELOAD_CONFIG_SEC = 60;

    public static final String CODE_LANGUAGE = "code_language";
    public static final String CHECK_CONTENT = "check_content";
    public static final String CHECK_FLAG = "check_flag";
    public static final String LIB_DIR = "lib_dir";
    public static final String SOURCE_DIR = "source_dir";
    public static final String DEFAULT_SOURCE_DIR = "";
    public static final String DELIMITER = ";";

    public static final String BLACKLIST_GROUP_NAME = "blacklist_group_name";
    public static final String BLACKLIST = "blacklist";
    public static final String BLACKLIST_OP = "blacklist_op";
    public static final String BLACKLIST_OP_ADD = "add";
    public static final String BLACKLIST_OP_DELETE = "delete";
    public static final String BLACKLIST_OP_LIST = "list";

    public static final String REGEX_MODE = "regex_mode";
    public static final String JAVA_PATTERN_MODE = "java_pattern";
    public static final String HYPER_SCAN_MODE = "hyper_scan";
    public static final String DEAFAULT_REGEX_MODE = JAVA_PATTERN_MODE;

    public static final String PARSER_GROUP_NAME = "parser_group_name";
    public static final String PARSER_OP = "parser_op";
    public static final String PARSER_OP_ADD = "add";
    public static final String PARSER_OP_DELETE = "delete";
    public static final String PARSER_OP_LIST = "list";
}
