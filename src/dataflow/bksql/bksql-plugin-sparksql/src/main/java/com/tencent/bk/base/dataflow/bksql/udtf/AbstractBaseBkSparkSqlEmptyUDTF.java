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

package com.tencent.bk.base.dataflow.bksql.udtf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

public abstract class AbstractBaseBkSparkSqlEmptyUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) {

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        if (null != getReturnTypes()) {
            for (int i = 0; i < getReturnTypes().length; i++) {
                fieldNames.add("__udtf_col_" + i);
                String type = getReturnTypes()[i];
                switch (type.toLowerCase()) {
                    case "integer":
                    case "int": {
                        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                        break;
                    }
                    case "bigint":
                    case "long": {
                        fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                        break;
                    }
                    case "float": {
                        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
                        break;
                    }
                    case "double": {
                        fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
                        break;
                    }
                    case "string": {
                        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                        break;
                    }
                    default: {
                        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                        break;
                    }
                }
            }
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    public abstract String[] getReturnTypes();


    @Override
    public void process(Object[] args) throws HiveException {

    }

    @Override
    public void close() throws HiveException {

    }
}
