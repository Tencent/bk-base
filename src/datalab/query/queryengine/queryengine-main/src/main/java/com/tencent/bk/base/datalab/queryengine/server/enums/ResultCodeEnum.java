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

package com.tencent.bk.base.datalab.queryengine.server.enums;

/**
 * API 统一返回状态码
 */
public enum ResultCodeEnum {

    /* 成功返回码 */
    SUCCESS("00", "成功"),

    /* 查询异常 1532001 ～ 1532099*/
    SQL_ERROR_TABLE_NOT_FOUND("1532001", "结果表不存在"),
    SQL_ERROR_REAL_TABLE_NOT_FOUND("1532002", "结果表存在，物理表不存在"),
    SQL_ERROR_RESPONSE_TIMEOUT("1532003", "查询超时"),
    QUERY_PERMISSION_INVALID("1532004", "没有权限执行"),
    QUERY_SQL_TOKEN_ERROR("1532005", "SQL词法错误"),
    QUERY_SQL_SYNTAX_ERROR("1532006", "SQL语法错误"),
    QUERY_DEVICE_NOT_SUPPORT("1532007", "该存储查询不支持检索"),
    QUERY_TIME_FIELD_ERROR("1532008", "时间字段处理异常"),
    QUERY_FILED_NOT_EXSITS("1532009", "字段不存在异常"),
    QUERY_DEVICE_ERROR("1532010", "查询失败"),
    QUERY_CONCURENTCY_SYNTAX_ERROR("1532011", "请求并发过高异常"),
    QUERY_OTHER_ERROR("1532012", "其他查询异常"),
    QUERY_THRID_INTERFACE_ERROR("1532013", "依赖接口异常"),
    QUERY_FUNC_NOT_EXISTS("1532014", "函数不存在异常"),
    QUERY_SEMANTIC_ERROR("1532015", "SQL语义检查失败"),
    QUERY_CONNECT_TIMEOUT_ERROR("1532016", "数据库连接超时"),
    QUERY_CONNECT_OTHER_ERROR("1532017", "数据库连接异常"),
    QUERY_NO_STORAGES_ERROR("1532018", "结果表没有配置存储"),
    QUERY_CATALOG_NOT_FOUND("1532019", "数据源不存在"),
    QUERY_SCHEMA_NOT_FOUND("1532020", "数据库不存在"),
    QUERY_EXCEED_MAX_RESPONSE_SIZE("1532021", "返回结果集超过最大值"),
    QUERY_NO_DATA_WRITTEN("1532022", "结果表对应的物理表不存在，请确认是否有数据入库"),

    /* 请求参数异常：1532100 ～ 1532199 */
    PARAM_IS_INVALID("1532100", "参数无效"),

    PARAM_IS_BLANK("1532101", "参数为空"),

    PARAM_TYPE_BIND_ERROR("1532102", "参数类型错误"),

    PARAM_NOT_COMPLETE("1532103", "参数缺失"),

    /* 系统权限错误：1532201 ～ 1532299*/
    USER_NOT_LOGGED_IN("1532201", "用户未登录"),

    USER_LOGIN_ERROR("1532202", "账号不存在或密码错误"),

    USER_ACCOUNT_FORBIDDEN("1532203", "账号已被禁用"),

    USER_NOT_EXIST("1532204", "用户不存在"),

    USER_HAS_EXISTED("1532205", "用户已存在"),

    LOGIN_CREDENTIAL_EXISTED("1532206", "凭证已存在"),

    PERMISSION_NO_ACCESS("1532207", "无访问权限"),

    RESOURCE_EXISTED("1532208", "资源已存在"),

    RESOURCE_NOT_EXISTED("1532209", "资源不存在"),

    DOWNLOAD_ACTION_FORBIDDEN("1532210", "数据下载已被禁用"),

    /* 业务模块异常：1532300 ～ 1532399 */
    QUERY_TEMPLATE_PROCESS_ERROR("1532300", "模板业务异常"),

    /* 系统错误：1532400 ～ 1532499 */
    SYSTEM_INNER_ERROR("1532400", "查询服务调用失败"),

    /* 数据库操作异常：1532500 ～ 1532599 */
    DATA_NOT_FOUND("1532501", "数据未找到"),

    DATA_IS_WRONG("1532502", "数据有误"),

    DATA_ALREADY_EXISTED("1532503", "数据已存在"),

    /* 第三方接口错误：1532600-1532699 */
    INTERFACE_FORBID_VISIT("1532600", "该接口禁止访问"),

    INTERFACE_ADDRESS_INVALID("1532601", "接口地址无效"),

    INTERFACE_REQUEST_TIMEOUT("1532602", "接口请求超时"),

    INTERFACE_EXCEED_LOAD("1532603", "接口负载过高"),

    INTERFACE_INNER_INVOKE_ERROR("1532604", "内部系统接口调用异常"),

    INTERFACE_AUTHAPI_INVOKE_ERROR("1532605", "内部系统auth接口调用异常"),

    INTERFACE_METAAPI_INVOKE_ERROR("1532606", "内部系统meta接口调用异常"),

    INTERFACE_STOREKITAPI_INVOKE_ERROR("1532607", "内部系统storekit接口调用异常"),

    INTERFACE_LABAPI_INVOKE_ERROR("1532609", "内部系统datalab接口调用异常"),

    INTERFACE_BATCHAPI_INVOKE_ERROR("1532610", "内部系统batch接口调用异常"),

    INTERFACE_BKSQL_INVOKE_ERROR("1532611", "BKSQL协议调用异常"),

    INTERFACE_IDEX_INVOKE_ERROR("1532612", "Idex接口调用异常");

    private String code;
    private String message;

    ResultCodeEnum(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String code() {
        return this.code;
    }

    public String message() {
        return message;
    }

    @Override
    public String toString() {
        return this.name();
    }
}
