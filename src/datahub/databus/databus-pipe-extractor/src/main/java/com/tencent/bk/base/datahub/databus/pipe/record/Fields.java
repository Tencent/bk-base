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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Fields implements Iterable<Object>, Serializable {

    //字段实体
    private List<Object> fields; //Field or Fields
    // 维护着Object => Field映射关系
    private Map<Object, Integer> index = new HashMap<>();

    static final long serialVersionUID = 19394238488238L;

    public Fields() {
        this.fields = new ArrayList<Object>();
    }

    /**
     * 解析配置文件阶段，追加schema信息.
     */
    public void append(Field field) {
        //todo: 这里要添加字段名唯一性校验
        this.fields.add(field);
        index();
    }

    public void append(Fields fields) {
        this.fields.add(fields);
        index();
    }


    public void set(int index, Field field) {
        this.fields.set(index, field);
        index();
    }


    public int size() {
        return this.fields.size();
    }

    public List<Object> toList() {
        return new ArrayList<Object>(this.fields);
    }

    public Object get(int index) {
        return this.fields.get(index);
    }

    @Override
    public Iterator<Object> iterator() {
        return this.fields.iterator();
    }

    /**
     * 根据字段寻找在schema的位置索引.
     */
    public Integer fieldIndex(Object field) {
        Integer ret = this.index.get(field);
        if (ret == null) {
            throw new IllegalArgumentException(field + " does not exist");
        }
        return ret;
    }

    /**
     * 重索引schema
     */
    public void index() {
        for (int i = 0; i < this.fields.size(); i++) {
            this.index.put(this.fields.get(i), i);
            if (this.fields.get(i) instanceof Field) {
                this.index.put(((Field) this.fields.get(i)).getName(), i);
            }
        }
    }

    @Override
    public String toString() {
        return this.fields.toString();
    }

    /**
     * 获取简化的字段列表,包含字段名称,字段类型,按照逗号分隔
     *
     * @return 逗号分隔的字段列表
     */
    public List<String> getSimpleDesc() {
        List<String> result = new ArrayList<>();
        for (Object obj : fields) {
            if (obj instanceof Field) {
                Field field = (Field) obj;
                result.add(field.getName() + "(" + field.getType() + ")");
            }
        }

        return result;
    }

    /**
     * 根据名称搜索字段.
     */
    public Field searchFieldByName(String name) {
        try {
            int idx = fieldIndex(name);
            return (Field) this.get(idx);
        } catch (IllegalArgumentException e) {
            for (Object obj : this.fields) {
                if (obj instanceof Fields) {
                    Field ret = ((Fields) obj).searchFieldByName(name);
                    if (ret != null) {
                        return ret;
                    }
                }
            }
        }

        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Fields fields1 = (Fields) o;
        return fields != null ? fields.equals(fields1.fields) : fields1.fields == null;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    public boolean contain(Object o) {
        return fields.contains(o);
    }
}
