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

package com.tencent.bk.base.dataflow.spark.sql.udaf

import com.tencent.bk.base.dataflow.core.function.base.udaf.{AbstractAggBuffer, AbstractAggFunction}
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFTypes.UcDataType
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

/**
  * 参数1：第一个string字段
  * 参数2：分割符
  * 参数3：第二个string字段
  * 参数4：升降序标志 "asc"|"desc"，大小写不敏感，默认降序，不在可选值范围升序
  * 参数5：是否过滤相同值，"distinct"为唯一可选值，表示过滤相同值，仅保留一个，不在可选值范围不过滤相同值
  *
  * example1: GroupConcat(s1) as new_field
  * ('1') ('2') ('3') ==> '1,2,3'
  * example2: GroupConcat(s1, '|') as new_field
  * ('1', '|') ('2', '|') ('3', '|') ==> '1|2|3'
  * example3: GroupConcat(s1, '|', s2) as new_field
  * 先zip，然后根据第二个字段降序，再取第一个字段值形成list，再切分
  * ('11', '|', '12') ('21', '|', '22') ('31', '|', '32') ==> '31|21|11'
  * example4: GroupConcat(s1, '|', s2, 'asc') as new_field
  * 先zip，然后根据第二个字段升序，再取第一个字段值形成list，再切分
  * ('11', '|', '12', 'asc') ('21', '|', '22', 'asc') ('31', '|', '32', 'asc') ==> '11|21|31'
  * example5: GroupConcat(s1, '|', s2, 'desc', 'distinct') as new_field
  * 先zip，然后根据第二个字段降序，再取第一个字段值形成list，再distinct取不同值，再切分
  * ('11', '|', '12', 'asc', 'distinct') ('21', '|', '22', 'asc', 'distinct') ('21', '|', '22', 'asc', 'distinct') ('31', '|', '32', 'asc', 'distinct') ==> '31|21|11'
  */
class GroupConcatScala extends AbstractAggFunction[UTF8String, GroupConcatScala] {

//  var separator = ","
//  var orderBy = "desc"
//  var flag = false
//  var isDistinct = false

  /**
    * 获取输入字段类型
    *
    * @return
    */
  override def getInputTypeArray: Array[UcDataType] = Array[UcDataType](
    UcDataType.STRING,
    UcDataType.STRING,
    UcDataType.STRING,
    UcDataType.STRING,
    UcDataType.STRING,
    UcDataType.STRING
  )

  /**
    * 获取buffer类型列表
    *
    * @return
    */
  override def getBufferTypeArray: Array[UcDataType] = Array[UcDataType](
    UcDataType.LIST_STRING,
    UcDataType.LIST_STRING,

    UcDataType.STRING,
    UcDataType.STRING,
    UcDataType.BOOLEAN,
    UcDataType.BOOLEAN
  )

  /**
    * 初始化函数，一般用于buffer的初始值设置
    */
  override protected def initialize(buffer: AbstractAggBuffer[_, _]): Unit = {
    buffer.update(0, mutable.ArrayBuffer.empty[String])
    buffer.update(1, mutable.ArrayBuffer.empty[String])

    // separator
    buffer.update(2, "");
    // orderBy
    buffer.update(3, "desc");
    // isDistinct
    buffer.update(4, false);
    // needSort
    buffer.update(5, false);
  }

  /**
    * 获取聚合后结果值
    *
    * @return
    */
  override def evaluate(buffer: AbstractAggBuffer[_, _]): UTF8String = UTF8String.fromString(
    buffer.getSeq(0).mkString(buffer.get(2).toString)
  )


  /**
    * UDAF更新操作
    *
    * @param inputValue
    */
  override def update(buffer: AbstractAggBuffer[_, _], inputValue: AnyRef*): Unit = {
    if (inputValue.size >= 1 && inputValue(0) != null) {
      if (inputValue.size >= 3 && inputValue(2) != null) {
        buffer.update(5, true);
        if (inputValue.size >= 5 && inputValue(4) != null) inputValue(4).toString.toLowerCase match {
          case "distinct" => {
            buffer.update(4, true);
            if (!buffer.getSeq(0).contains(inputValue(0))) {
              buffer(1) = buffer.getSeq(1) :+ inputValue(2)
              buffer(0) = buffer.getSeq(0) :+ inputValue(0)
            }
          }
          case _ => {
            // TODO: distinct写错也给它处理，这里不赋值最终会返回空
            buffer(1) = buffer.getSeq(1) :+ inputValue(2)
            buffer(0) = buffer.getSeq(0) :+ inputValue(0)
          }
        } else {
          buffer(1) = buffer.getSeq(1) :+ inputValue(2)
          buffer(0) = buffer.getSeq(0) :+ inputValue(0)
        }
      } else buffer(0) = buffer.getSeq(0) :+ inputValue(0)
    }
    if (inputValue.size >= 2 && inputValue(1) != null){
      buffer.update(2, inputValue(1).toString);
    }
    if (inputValue.size >= 4 && inputValue(3) != null)
      buffer.update(3, inputValue(3).toString);
  }

  /**
    * 合并两个UDAF对象的buffer
    *
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: AbstractAggBuffer[_, _], buffer2: AbstractAggBuffer[_, _]): Unit = {
    val seq =
      if (buffer2.getBoolean(5)) {
        val toConcat = buffer1.getSeq(0).toSeq ++ buffer2.getSeq(0).toSeq
        val toOrder = buffer1.getSeq(1).toSeq ++ buffer2.getSeq(1).toSeq
        val orderBy = buffer2.get(3).toString
        if (order(orderBy)) {
          buffer1(1) = toOrder.sortWith(
            (a, b) => a.toString.compareTo(b.toString) < 0)
          toConcat.zip(toOrder).sortWith(
            (x, y) => x._2.toString.compareTo(y._2.toString) < 0).map(_._1)
        }
        else {
          buffer1(1) = toConcat.sortWith(
            (a, b) => b.toString.compareTo(a.toString) < 0)
          toConcat.zip(toOrder).sortWith(
            (x, y) => x._2.toString.compareTo(y._2.toString) < 0).reverse.map(_._1)
        }
      } else
        buffer1.getSeq(0).toSeq ++ buffer2.getSeq(0).toSeq

    buffer1(4) = buffer2.getBoolean(4)
    buffer1(0) = if (buffer1.getBoolean(4)) seq.distinct else seq
    buffer1(2) = buffer2.get(2).toString
    buffer1(3) = buffer2.get(3).toString
  }


  def order(orderBy: String): Boolean = orderBy.toUpperCase match {
    case "DESC" => false
    case "ASC" => true
    case _ => true
  }
}
