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

package com.tencent.bk.base.datahub.databus.pipe.record;

import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;
import com.tencent.bk.base.datahub.databus.pipe.exception.TypeConversionError;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class Field {

    private String type;
    private String name;
    private Boolean isDimension;
    private Boolean isJson = false;
    private Boolean isOutputField = true;
    private Boolean isInternal = false;

    /**
     * field 默认无dimension.
     */
    public Field(String name, String type) {
        this(name, type, false, false);
    }

    /**
     * field 含dimension配置.
     */
    public Field(String name, String type, Boolean isDimension) {
        this(name, type, isDimension, false);
    }


    /**
     * field类型为string/text时，可以设置是否为json的属性
     *
     * @param name 字段名称
     * @param type 字段类型
     * @param isDimension 是否为维度字段
     * @param isJson 是否为json字符串
     */
    public Field(String name, String type, Boolean isDimension, Boolean isJson) {
        this.type = type.toLowerCase();
        this.name = name;
        this.isDimension = isDimension;
        this.isJson = isJson;
    }


    @Override
    public String toString() {
        return String.format("(name:%s,type:%s,is_dimension:%s,is_json:%s,is_internal:%s,is_output:%s)", name, type,
                isDimension, isJson, isInternal, isOutputField);
    }

    /**
     * 根据字段类型进行类型转换.
     */
    public Object castType(Object data) {
        if (data == null) {
            return null;
        }

        switch (type) {
            case EtlConsts.TEXT:
            case EtlConsts.STRING:
                if (isJson) {
                    if (data instanceof Map) {
                        return data;
                    } else {
                        throw new TypeConversionError(
                                String.format("can not convert %s to json %s. data type: %s", data, type,
                                        data.getClass()));
                    }
                } else {
                    return data.toString();
                }

            case EtlConsts.DOUBLE:
            case EtlConsts.LONG:
            case EtlConsts.INT:
            case EtlConsts.BIGINT:
            case EtlConsts.BIGDECIMAL:
                return parseNumber(data);

            default:
                return null;
        }
    }

    /**
     * return the type.
     *
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * 字段的名称.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    public Boolean isDimension() {
        return isDimension;
    }

    public Boolean isJson() {
        return isJson;
    }


    public void setIsOutputField(Boolean isOutputField) {
        this.isOutputField = isOutputField;
    }

    public Boolean isOutputField() {
        return isOutputField;
    }

    public void setIsInternal(boolean isInternal) {
        this.isInternal = isInternal;
    }

    public Boolean isInternal() {
        return isInternal;
    }

    /**
     * 将字符串转换为Integer
     *
     * @param value 字符串
     * @return 数值的Integer
     */
    private Integer getIntFromString(String value) {
        return Integer.parseInt(trimStringToInteger(value));
    }

    /**
     * 将字符串转换为Long
     *
     * @param value 字符串
     * @return 数值的Long
     */
    private Long getLongFromString(String value) {
        return Long.parseLong(trimStringToInteger(value));
    }

    /**
     * 将字符串转换为BigInteger
     *
     * @param value 字符串
     * @return 数值的BigInteger
     */
    private BigInteger getBigIntegerFromString(String value) {
        return new BigInteger(trimStringToInteger(value));
    }

    /**
     * 对即将要转换为整数的字符串进行处理，去掉双引号和小数点后面的内容
     *
     * @param value 待处理的字符串
     * @return 符合整数转换的字符串
     */
    private String trimStringToInteger(String value) {
        if (value.contains(".")) {
            value = StringUtils.split(value, ".")[0];
        }
        if (value.contains("\"")) {
            value = StringUtils.remove(value, "\"");
        }

        return value;
    }

    /**
     * 检查字符串是否为空串，或者null字符串
     *
     * @param str 待检查的字符串
     * @return true/false
     */
    private boolean isNullString(String str) {
        return StringUtils.isBlank(str) || str.equalsIgnoreCase(EtlConsts.NULL);
    }

    /**
     * 将输入的对象转换为数值类型（int、long、double和bigint），输入对象可以为string和number类型。
     *
     * @param data 将要转换为数值的对象
     * @return 数值对象，可能为int、long、double、bigint中的一种，或者null值
     */
    private Object parseNumber(Object data) {
        try {
            if (data instanceof String) {
                if (!isNullString(data.toString())) {
                    switch (type) {
                        case EtlConsts.INT:
                            return getIntFromString(data.toString().trim());
                        case EtlConsts.LONG:
                            return getLongFromString(data.toString().trim());
                        case EtlConsts.DOUBLE:
                            return Double.parseDouble(data.toString().trim());
                        case EtlConsts.BIGINT:
                            return getBigIntegerFromString(data.toString().trim());
                        case EtlConsts.BIGDECIMAL:
                            return new BigDecimal(StringUtils.remove(data.toString().trim(), "\""));
                        default:
                            return null;
                    }
                }
            } else if (data instanceof Number) {
                switch (type) {
                    case EtlConsts.INT:
                        return ((Number) data).intValue();
                    case EtlConsts.LONG:
                        return ((Number) data).longValue();
                    case EtlConsts.DOUBLE:
                        return ((Number) data).doubleValue();
                    case EtlConsts.BIGINT:
                        return new BigInteger(trimStringToInteger(data.toString()));
                    case EtlConsts.BIGDECIMAL:
                        return new BigDecimal(data.toString());
                    default:
                        return null;
                }
            } else {
                throw new TypeConversionError(
                        String.format("can not convert %s to %s. data type: %s", data, type, data.getClass()));
            }
        } catch (Exception e) {
            throw new TypeConversionError(
                    String.format("can not convert %s to %s. err: %s", data, type, e.getMessage()));
        }

        // 不支持的数据类型转换
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null) {
            return false;
        }

        if (getClass() != o.getClass()) {
            return false;
        }

        Field field = (Field) o;
        return type.equals(field.type) &&
                name.equals(field.name) &&
                isDimension.booleanValue() == field.isDimension.booleanValue() &&
                isJson.booleanValue() == field.isJson.booleanValue() &&
                isOutputField.booleanValue() == field.isOutputField.booleanValue() &&
                isInternal.booleanValue() == field.isInternal.booleanValue();
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + isDimension.hashCode();
        result = 31 * result + isJson.hashCode();
        result = 31 * result + isOutputField.hashCode();
        result = 31 * result + isInternal.hashCode();
        return result;
    }
}
