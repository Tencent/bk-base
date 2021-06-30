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

package com.tencent.bk.base.dataflow.spark.sql.udf;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ZipFieldsToRecords implements Serializable {
  private static final Logger logger = Logger.getLogger(ZipFieldsToRecords.class);

  /**
   * 将多个特定字段根据特定分隔符分割后合并，再分多行输出
   * example: zip('*','11_21','_','12_22','_','13_23','_') as rt ==> 11*12*13
   * ==> 21*22*23
   *
   * @param concatation
   * @param fieldAndDelim
   * @return
   */
  public String[] call(final String concatation, final String... fieldAndDelim) {
    try {
      if (fieldAndDelim == null || fieldAndDelim.length == 0 || fieldAndDelim.length % 2 != 0) {
        throw new Exception("待转换参数个数非偶数.");
      }
      // 用户存放字段分割后的字符数组
      int splitLength = 0;
      List<String[]> splitFields = new ArrayList<>();
      for (int i = 0; i < fieldAndDelim.length; i = i + 2) {
        String[] valueArray = fieldAndDelim[i].split(fieldAndDelim[i + 1], -1);
        if (splitLength == 0) {
          splitLength = valueArray.length;
        } else if (splitLength != valueArray.length) {
          throw new Exception("待转换参数值经切分后长度不一致.");
        }
        splitFields.add(valueArray);
      }
      List<String> concatFields = new ArrayList<>();
      for (int i = 0; i < splitLength; i++) {
        // 拼接为新的数组
        StringBuilder concatField = new StringBuilder();
        // m用于末尾是否拼接字符
        int m = 0;
        for (String[] splitField : splitFields) {
          if (m != splitFields.size() - 1) {
            concatField.append(splitField[i]).append(concatation);
          } else {
            concatField.append(splitField[i]);
          }
          m++;
        }
        concatFields.add(i, concatField.toString());
      }
      return concatFields.toArray(new String[concatFields.size()]);
    } catch (Throwable e2) {
      // 输入数据格式错误
      logger.error("zip error.", e2);
      return new String[] {null};
    }
  }

  /**
   * 一个测试方法
   * @param args
   */
  public static void main(String[] args) {
    String[] cc = ("null%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull, "
            + "%7C, 83%7C83%7C83%7C999%7C999%7C999%7C61%7C61%7C61%7C61%7C61%7C61, "
            + "%7C, 16.44%7C16.46%7C13.76%7C9.96%7C13.66%7C26.13%7C6.86%7C18.19%7C22.57%7C22.39%7C25.94%7C0.00, "
            + "%7C, 21.34%7C21.05%7C21.79%7C21.17%7C21.41%7C17.72%7C20.19%7C22.04%7C22.02%7C22.17%7C21.97%7C20.45, "
            + "%7C, null%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull, "
            + "%7C, 30%7C30%7C30%7C30%7C30%7C30%7C30%7C30%7C30%7C30%7C30%7C30, "
            + "%7C, 4%7C4%7C4%7C6%7C6%7C6%7C8%7C8%7C8%7C8%7C8%7C8, "
            + "%7C, 7993%7C7993%7C7993%7C7993%7C7993%7C7993%7C7993%7C7993%7C7993%7C7993%7C7993%7C7993, "
            + "%7C, 423%7C428%7C433%7C451%7C456%7C462%7C479%7C485%7C490%7C495%7C500%7C501, "
            + "%7C, 1%7C1%7C1%7C1%7C1%7C1%7C1%7C1%7C1%7C1%7C1%7C1, "
            + "%7C, 7%7C7%7C7%7C4%7C4%7C4%7C7%7C7%7C7%7C7%7C7%7C7, "
            + "%7C, 38.7%7C38.7%7C38.7%7C38.7%7C38.7%7C38.7%7C38.7%7C38.7%7C38.7%7C38.7%7C38.7%7C38.7, "
            + "%7C, %7C%7C%7C%7C%7CPQ_0%7C%7C%7C%7C%7C%7C, "
            + "%7C, 0%7C0%7C0%7C0%7C0%7C0%7C0%7C0%7C0%7C0%7C0%7C0, "
            + "%7C, null%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull, "
            + "%7C, null%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull, "
            + "%7C, null%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull%7Cnull, "
            + "%7C")
            .split(", ");

    ZipFieldsToRecords a = new ZipFieldsToRecords();
    a.call("&", cc);
    System.out.println("6666666");
  }
}
