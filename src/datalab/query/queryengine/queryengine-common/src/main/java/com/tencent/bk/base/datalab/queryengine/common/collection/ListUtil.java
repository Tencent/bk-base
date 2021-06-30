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

package com.tencent.bk.base.datalab.queryengine.common.collection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ListUtil {

    public static List diff(List ls, List ls2) {
        List list = new ArrayList(Arrays.asList(new Object[ls.size()]));
        Collections.copy(list, ls);
        list.removeAll(ls2);
        return list;
    }

    public static List intersect(List ls, List ls2) {
        List list = new ArrayList(Arrays.asList(new Object[ls.size()]));
        Collections.copy(list, ls);
        list.retainAll(ls2);
        return list;
    }

    public static boolean cmpList(List<?> l1, List<?> l2) {
        // make a copy of the list so the original list is not changed, and remove() is supported
        if (l1 == null || l2 == null || l1.size() != l2.size()) {
            return false;
        }
        ArrayList<?> cp = new ArrayList<>(l1);
        for (Object o : l2) {
            if (!cp.remove(o)) {
                return false;
            }
        }
        return cp.isEmpty();
    }

    public static boolean cmpMap(Map<String, List<String>> map1, Map<String, List<String>> map2) {
        if (map1 == null || map2 == null || map1.size() != map2.size()) {
            return false;
        }
        Iterator iter = map1.entrySet()
                .iterator();
        while (iter.hasNext()) {
            Map.Entry<String, List<String>> entry = (Map.Entry<String, List<String>>) iter.next();
            List<String> val1 = entry.getValue();
            List<String> val2 = map2.get(entry.getKey());
            if (!cmpList(val1, val2)) {
                return false;
            }
        }
        return true;
    }
}
