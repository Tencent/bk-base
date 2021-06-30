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

package com.tencent.bk.base.datalab.queryengine.server.util;

import com.google.common.collect.Lists;
import com.tencent.bk.base.datalab.meta.Field;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultTableFiledTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.meta.FieldMeta;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * 元数据工具类
 */
@Slf4j
public class SchemaUtil {

    /**
     * 生成元数据字段列表
     *
     * @param relFields calcite rel 字段列表
     * @return 元数据字段列表
     */
    public static List<FieldMeta> generateRtFields(List<RelDataTypeField> relFields) {
        if (relFields == null) {
            return null;
        }
        List<FieldMeta> fieldMetaList = Lists.newArrayList();
        for (int i = 0; i < relFields.size(); i++) {
            RelDataTypeField f = relFields.get(i);
            String fieldName = f.getName();
            FieldMeta.FieldMetaBuilder fb = FieldMeta.builder()
                    .fieldIndex(i)
                    .fieldName(fieldName)
                    .fieldAlias(fieldName);
            SqlTypeName fieldType = f.getType()
                    .getSqlTypeName();
            ResultTableFiledTypeEnum rtType = TypeConvertUtil.getRtTypeByRelType(fieldType);
            if (rtType == null) {
                log.error(
                        String.format("Not support columnType:%s [%s:%s]", fieldType.getName(),
                                fieldName, fieldType.getName()));
                throw new QueryDetailException(ResultCodeEnum.PARAM_TYPE_BIND_ERROR,
                        String.format("Not support columnType:%s [%s:%s]", fieldType.getName(),
                                fieldName, fieldType.getName()));
            }
            fb.fieldType(rtType.toString());
            fieldMetaList.add(fb.build());
        }
        return fieldMetaList;
    }

    /**
     * 获取 calcite schema 元数据
     *
     * @param fields meta 元数据字段列表
     * @param typeFactory SqlTypeFactoryImpl 实例
     * @param typeSystem RelDataTypeSystem 实例
     * @return table schema
     */
    public static AbstractTable generateCalciteSchema(List<Field> fields,
            SqlTypeFactoryImpl typeFactory, RelDataTypeSystem typeSystem) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory tf) {
                RelDataTypeFactory.Builder builder = new RelDataTypeFactory
                        .Builder(typeFactory);
                fields.forEach(f -> {
                    String fieldType = f.getFieldType();
                    String fieldName = f.getFieldName();
                    //内部时间字段timestamp，需要过滤掉
                    if (ResultTableFiledTypeEnum.TIMESTAMP.toString()
                            .equals(fieldType.toLowerCase())) {
                        return;
                    }
                    SqlTypeName relType = TypeConvertUtil.getRelTypeByRtType(fieldType);
                    if (relType == null) {
                        log.error(String.format("Not support columnType:%s [%s:%s]", fieldType,
                                fieldName, fieldType));
                        throw new QueryDetailException(ResultCodeEnum.PARAM_TYPE_BIND_ERROR,
                                String.format("Not support columnType:%s [%s:%s]", fieldType,
                                        fieldName, fieldType));
                    }
                    builder.add(fieldName, new BasicSqlType(typeSystem, relType));
                });
                return builder.build();
            }
        };
    }
}
