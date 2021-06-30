
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

package com.tencent.bk.base.datahub.databus.pipe;

public class EtlConsts {

    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_TIMEZONE = "UTC";
    public static final String DISPLAY_TIMEZONE = "databus.display.timezone";

    public static final String CC_CACHE = "cc_cache";
    public static final String DATA_ID = "data_id";
    public static final String COND = "cond";
    public static final String CONVERT = "convert";
    public static final String DERIVE = "derive";
    public static final String EXTRACT = "extract";
    public static final String CALCULATE = "calculate";
    public static final String DISPATCH = "dispatch";
    public static final String NEXT = "next";
    public static final String METHOD = "method";
    public static final String FROM_JSON = "from_json";
    public static final String ITERATE = "iterate";
    public static final String SELECTOR = "selector";
    public static final String POP = "pop";
    public static final String FROM_URL = "from_url";
    public static final String PARSE = "parse";
    public static final String SPLIT = "split";
    public static final String FROM_JSON_LIST = "from_json_list";
    public static final String REPLACE = "replace";
    public static final String SPLITKV = "splitkv";
    public static final String ITEMS = "items";
    public static final String ZIP = "zip";
    public static final String TO_STRING = "to_string";
    public static final String ARGS = "args";
    public static final String DIMENSION = "dimension";
    public static final String REGEX_EXTRACT = "regex_extract";
    public static final String REGEXP = "regexp";

    public static final String BOOL = "bool";
    public static final String LVAL = "lval";
    public static final String RVAL = "rval";
    public static final String OP = "op";
    public static final String EQUAL = "equal";
    public static final String NOT_EQUAL = "not equal";
    public static final String AND = "and";
    public static final String OR = "or";
    public static final String NOT = "not";
    public static final String VALUE = "value";
    public static final String FIELD = "field";
    public static final String NAME = "name";
    public static final String VAL = "val";
    public static final String TRIM = "trim";

    public static final String EXPR = "expr";
    public static final String FUN_CALL = "fun_call";
    public static final String MAP = "map";
    public static final String MINUS = "minus";
    public static final String PERCENT = "percent";
    public static final String MAX = "max";
    public static final String SUBTYPE = "subtype";
    public static final String IS_DIMENSION = "is_dimension";
    public static final String KEY = "key";
    public static final String INDEX = "index";
    public static final String KEYS = "keys";
    public static final String SEPARATOR = "separator";
    public static final String DEFAULT_TYPE = "default_type";
    public static final String DEFAULT_VALUE = "default_value";


    public static final String TYPE = "type";
    public static final String FUN = "fun";
    public static final String ACCESS = "access";
    public static final String ASSIGN = "assign";
    public static final String BRANCH = "branch";
    public static final String ASSIGN_TYPE = "assign_type";
    public static final String ASSIGN_VAR = "assign_var";
    public static final String ASSIGN_OBJ = "assign_obj";
    public static final String ASSIGN_POS = "assign_pos";
    public static final String ASSIGN_JSON = "assign_json";
    public static final String ASSIGN_VALUE = "assign_value";
    public static final String ASSIGN_TO = "assign_to";
    public static final String ACCESS_POS = "access_pos";
    public static final String ACCESS_MULTI = "access_multi";
    public static final String ACCESS_OBJ = "access_obj";
    public static final String CSV_LINE = "csvline";

    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USER = "user";
    public static final String PASSWD = "passwd";
    public static final String DB = "db";


    public static final String CONF = "conf";
    public static final String TIME_FIELD_NAME = "time_field_name";
    public static final String DEFAULT_TIME_FIELD_NAME = "localTime";
    public static final String DEFAULT_OUTPUT_FIELD_NAME = "timestamp";
    public static final int DEFAULT_TIME_FIELD_INDEX = Integer.MIN_VALUE;
    public static final String OUTPUT_FIELD_NAME = "output_field_name";
    public static final String TIME_FORMAT = "time_format";
    public static final String TIMESTAMP_LEN = "timestamp_len";
    public static final String TIMEZONE = "timezone";
    public static final String TIME_ROUND = "time_round";
    public static final String DELIMITER = "delimiter";
    public static final String DERIVE_DIMENSION_FIELD_NAME = "derive_dimension_field_name";
    public static final String DERIVE_VALUE_FIELD_NAME = "derive_value_field_name";
    public static final String TIMESTAMP = "timestamp";

    public static final String ENCODING = "encoding";
    public static final String UTF8 = "UTF8";
    public static final String COMPRESSED = "compressed";
    public static final String ITERATION_IDX = "_iteration_idx";
    public static final String IGNORE_FIELDS = "ignore_fields";

    public static final String INT = "int";
    public static final String STRING = "string";
    public static final String LONG = "long";
    public static final String BIGINT = "bigint";
    public static final String BIGDECIMAL = "bigdecimal";
    public static final String DOUBLE = "double";
    public static final String TEXT = "text";
    public static final String NULL = "null";

    public static final String RESULT = "result";
    public static final String SCHEMA = "schema";
    public static final String NODES = "nodes";
    public static final String ERRORS = "errors";
    public static final String ERROR_MESSAGE = "error_message";
    public static final String OUTPUT_TYPE = "output_type";
    public static final String LABEL = "label";
    public static final String INTER_FIELDS = "inter_field";
    public static final String USER_FIELDS = "user_field";
    public static final String HIDDEN = "hidden";
    public static final String DISPLAY = "display";

    public static final String GLOBAL_LABEL = "__global__";
    public static final String INTERNAL_LABEL = "__internal__";
    public static final String ALL_KEYS = "__all_keys__";

    public static final String LOGIC_EQUAL = "equal";
    public static final String LOGIC_NOT_EQUAL = "not_equal";
    public static final String LOGIC_GREATER = "greater";
    public static final String LOGIC_NOT_GREATER = "not_greater";
    public static final String LOGIC_LESS = "less";
    public static final String LOGIC_NOT_LESS = "not_less";
    public static final String LOGIC_STARTSWITH = "startswith";
    public static final String LOGIC_ENDSWITH = "endswith";
    public static final String LOGIC_CONTAINS = "contains";

}
