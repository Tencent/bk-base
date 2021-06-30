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

package com.tencent.bk.base.datalab.queryengine.common.character;


import com.google.common.base.Charsets;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class MultiStringUtil extends StringUtils {

    /**
     * 判断字符串列表是否包含空串
     *
     * @param str 字符串列表
     * @return 是否包含空串
     */
    public static boolean isListNotBank(String... str) {
        if (str == null) {
            return false;
        }
        for (String s : str) {
            if (StringUtils.isBlank(s)) {
                return false;
            }
        }
        return true;
    }

    /***
     * 把一个字符串分离开，并按照 key/value 形式保存到Map中
     * @param map 字符串中的字符存放在map中
     * @param data 要分离的字符串
     * @param encoding 字符编码
     * @throws UnsupportedEncodingException  不支持的编码异常
     */
    public static void parseParameters(Map<String, String> map, String data, String encoding)
            throws UnsupportedEncodingException {
        //将字符串转换为字节数组
        if ((data != null) && (data.length() > 0)) {
            byte[] bytes;
            bytes = data.getBytes(Charsets.UTF_8);
            parseParameters(map, bytes, encoding);
        }

    }

    /****
     * 将字节数组中的字符分离到 map 中，该方法支持字符分离的标记:'%',
     * '?','&'
     *
     * @param map 存放分离的字符Map
     * @param data  分离的字节数组
     * @param encoding 按什么编码方法
     * @throws UnsupportedEncodingException
     */
    public static void parseParameters(Map<String, String> map, byte[] data, String encoding)
            throws UnsupportedEncodingException {

        if (data != null && data.length > 0) {
            int ix = 0;
            int ox = 0;
            String key = null;
            String value = null;
            //对分离的字符数组循环
            while (ix < data.length) {
                byte c = data[ix++];
                //当字节数组中的元素，遇到'%','?','&'字符，就创建value字符串
                switch ((char) c) {
                    case '%':
                    case '?':
                    case '&':
                        value = new String(data, 0, ox, encoding);
                        if (key != null) {
                            putMapEntry(map, key, value);
                            key = null;
                        }
                        ox = 0;
                        break;
                    //当字节数组中的元素，遇到'='字符，那么创建key字符串
                    case '=':
                        key = new String(data, 0, ox, encoding);
                        ox = 0;
                        break;
                    //当字节数组中的元素，遇到'+'，那么就是以' '代替
                    case '+':
                        data[ox++] = (byte) ' ';
                        break;
                    default:
                        data[ox++] = c;
                }
            }
            //最后一个截取的字符串，要进行处理
            if (key != null) {
                value = new String(data, 0, ox, encoding);
                putMapEntry(map, key, value);
            }
        }
    }

    /****
     * 把分离的小字符串存放在 map 中
     *
     * @param map Map实例
     * @param name key
     * @param value value
     */
    private static void putMapEntry(Map<String, String> map, String name, String value) {
        map.put(name, value);
    }

    /**
     * 字节数组合并
     *
     * @param byteLeft 待合并的字节数组
     * @param byteRight 待合并的字节数组
     * @return 合并后的字节数组
     */
    public static byte[] byteMerger(byte[] byteLeft, byte[] byteRight) {
        byte[] byteMerged = new byte[byteLeft.length + byteRight.length];
        System.arraycopy(byteLeft, 0, byteMerged, 0, byteLeft.length);
        System.arraycopy(byteRight, 0, byteMerged, byteLeft.length, byteRight.length);
        return byteMerged;
    }
}
