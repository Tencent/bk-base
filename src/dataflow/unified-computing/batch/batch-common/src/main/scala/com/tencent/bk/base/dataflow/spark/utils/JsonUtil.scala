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

package com.tencent.bk.base.dataflow.spark.utils

import com.tencent.bk.base.dataflow.spark.exception.BatchException
import com.tencent.bk.base.dataflow.spark.exception.code.ErrorCode

import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

/**
  * Created by Tencent BlueKing
  */
object JsonUtil {

  def mapToJsonStr(msg: Map[String, Any]): String = {
    JSONObject(msg).toString()
  }

  def listToJsonStr(msg: scala.List[scala.Any]): String = {
    JSONArray(msg).toString()
  }

  def jsonToMap(json: String): Map[String, Any] = {
    JSON.parseFull(json) match {
      case Some(map: scala.collection.immutable.Map[String, Any]) => map
      case None => {
        val errMsg = s"parse json failed : ${json}"
        throw new BatchException(s"parse json failed : ${json}", ErrorCode.ILLEGAL_ARGUMENT)
      }
      case other => {
        val errMsg = s"Unknow structs : ${other}"
        throw new BatchException(s"Unknow structs : ${other}", ErrorCode.ILLEGAL_ARGUMENT)
      }
    }
  }

}
