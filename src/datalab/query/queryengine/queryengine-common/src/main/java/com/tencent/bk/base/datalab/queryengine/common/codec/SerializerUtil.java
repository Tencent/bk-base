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

package com.tencent.bk.base.datalab.queryengine.common.codec;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SerializerUtil {

    /**
     * 对象反序列化方法
     *
     * @param bytes 需要反序列化的字节数组
     * @param <T> 需要反序列化的类
     * @return 反序列化后的类实例
     */
    public static <T extends Serializable> T deserialize(byte[] bytes) {
        T obj = null;
        try (ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
                ObjectInputStream oi = new ObjectInputStream(bi)) {
            obj = (T) oi.readObject();
            return obj;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return obj;
    }

    /**
     * 对象序列化方法
     *
     * @param obj 需要序列化的对象
     * @return 序列化后的字节数组
     */
    public static byte[] serialize(Serializable obj) {
        byte[] bytes = null;

        try (ByteArrayOutputStream bo = new ByteArrayOutputStream();
                ObjectOutputStream oo = new ObjectOutputStream(bo);) {
            oo.writeObject(obj);
            bytes = bo.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }
}