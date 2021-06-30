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

package com.tencent.bk.base.dataflow.flink.schema;

import com.tencent.bk.base.dataflow.flink.schema.type.BigIntType;
import com.tencent.bk.base.dataflow.flink.schema.type.DoubleType;
import com.tencent.bk.base.dataflow.flink.schema.type.FloatType;
import com.tencent.bk.base.dataflow.flink.schema.type.IntType;
import com.tencent.bk.base.dataflow.flink.schema.type.LongType;
import com.tencent.bk.base.dataflow.flink.schema.type.StringType;
import com.tencent.bk.base.dataflow.core.struct.schema.ISchema;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class SchemaFactory {

    private static Map<String, ISchema> schemaMap = new HashMap<String, ISchema>();

    // 注册schema类型
    static {
        schemaMap.put("bigint", new BigIntType());
        schemaMap.put("double", new DoubleType());
        schemaMap.put("float", new FloatType());
        schemaMap.put("int", new IntType());
        schemaMap.put("long", new LongType());
        schemaMap.put("string", new StringType());
    }

    /**
     * 通过字段类型获取flink内置的schema类型
     *
     * @param type 字段类型
     * @return schema类型
     */
    public TypeInformation<?> callTypeInfo(String type) {
        return (TypeInformation<?>) schemaMap.get(type).callTypeInfo(type);
    }

    /**
     * 获取node对应的type info
     *
     * @param node node
     * @return type info
     */
    public TypeInformation<?>[] getFieldsTypes(Node node) {
        List<TypeInformation<?>> fieldsTypesList = new ArrayList<>();
        for (NodeField nodeField : node.getFields()) {
            fieldsTypesList.add(callTypeInfo(nodeField.getType()));
        }
        return fieldsTypesList.toArray(new TypeInformation<?>[0]);
    }
}
