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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._

object TestSQLAnalyzer {

  def main(args: Array[String]): Unit = {

    sqlAnalyzer(
      """
        |select *
        |from test1
      """.stripMargin
    )

    sqlAnalyzer(
      """
        |select *
        |from test1
        |where thedate='20190611'
      """.stripMargin
    )

    sqlAnalyzer(
      """
        |select thedate, dtEventTime, f_int, f_double
        |from test1
        |where thedate='20190611'
      """.stripMargin
    )

    sqlAnalyzer(
      """
        |select thedate, dtEventTime, f_int, f_double
        |from test1
        |where thedate='20190611'
      """.stripMargin
    )

    // agg
    sqlAnalyzer(
      """
        |select
        |count(*) as cnt1,
        |count(1) as cnt2,
        |sum(f_int) as sum_i,
        |sum(f_long) as sum_l,
        |sum(f_float) as sum_f,
        |sum(f_double) as sum_d,
        |avg(f_long) as avg_l,
        |max(f_long) as max_l,
        |min(f_long) as min_l
        |from test1
        |where thedate='20190611'
      """.stripMargin
    )

    // join
    sqlAnalyzer(
      """
        |select t1.*, t2.* from test1 t1, test2 t2 where t1.dtEventTimeStamp = t2.dtEventTimeStamp
      """.stripMargin
    )

    // join
    sqlAnalyzer(
      """
        |select t1.f_int, t1.f_long, t2.thedate, t2.localTime from test1 t1 left join test2 t2
        |on (t1.dtEventTimeStamp = t2.dtEventTimeStamp)
      """.stripMargin
    )

    // union
    sqlAnalyzer(
      """
        |select * from test1
        |union
        |select * from test2
      """.stripMargin
    )
    // union all
    sqlAnalyzer(
      """
        |select * from test1
        |union all
        |select * from test2
      """.stripMargin
    )

    // sub query
    sqlAnalyzer(
      """
        |select
        | thedate,
        | count(*) as cnt,
        | sum(f_int) as sum_i
        | from (
        |select t1.f_int, t1.f_long, t2.thedate, t2.localTime from test1 t1 left join test2 t2
        |on (t1.dtEventTimeStamp = t2.dtEventTimeStamp)
        |  )
        | group by  thedate
      """.stripMargin
    )

    // grouping sets
    sqlAnalyzer(
      """
        |select
        | A,B,C,count(*) as cnt
        | from test4
        | group by A,B,C grouping sets((A,B),(A,C))
      """.stripMargin
    )

    // cube
    sqlAnalyzer(
      """
        |select
        | A,B,C,count(*) as cnt
        | from test4
        | group by A,B,C with cube
      """.stripMargin
    )

    // cube
    sqlAnalyzer(
      """
        |select
        | A,B,C,count(*) as cnt
        | from test4
        | group by cube(A,B,C)
      """.stripMargin
    )


    // rollup
    sqlAnalyzer(
      """
        |select
        | A,B,C,count(*) as cnt
        | from test4
        | group by A,B,C with rollup
      """.stripMargin
    )

    sqlAnalyzer(
      """
        |select
        | A,B,C,count(*) as cnt
        | from test4
        | group by rollup(A,B,C)
      """.stripMargin
    )

    // rollup GROUPING__ID
    sqlAnalyzer(
      """
        |select
        | A,B,C,
        | GROUPING__ID,
        | count(*) as cnt
        | from test4
        | group by A,B,C with rollup
      """.stripMargin
    )

    // window function
    sqlAnalyzer(
      """
        |select f.udid,
        |       f.from_id,
        |       f.ins_date
        |from
        |  (select /* +MAPJOIN(u) */ u.device_id as udid,
        |                            g.device_id as gdid,
        |                            u.from_id,
        |                            u.ins_date,
        |                            row_number() over (partition by u.device_id
        |                                               order by u.ins_date asc) as row_number
        |   from user_device_info u
        |   left outer join
        |     (select device_id
        |      from 3g_device_id
        |      where log_date<'2013-07-25') g on (u.device_id = g.device_id)
        |   where u.log_date='2013-07-25'
        |     and u.device_id is not null
        |     and u.device_id <> '') f
        |where f.gdid is null
        |  and row_number=1
      """.stripMargin
    )

    // sub query
    sqlAnalyzer(
      """
        |select (select (select 1) + 1) + 1 as s1
      """.stripMargin)

    // with
    sqlAnalyzer(
      """
        |with t2 as (with t1 as (select 1 as b, 2 as c) select b, c from t1)
        | select a from (select 1 as a union all select 2 as a) t
        | where a = (select max(b) from t2)
      """.stripMargin)

    // with
    sqlAnalyzer(
      """
        |with t2 as (select 1 as b, 2 as c)
        | select a from (select 1 as a union all select 2 as a) t
        | where a = (select max(b) from t2)
      """.stripMargin)


    // 笛卡尔积关联（CROSS JOIN）
    sqlAnalyzer(
      """
        | select
        | a.thedate,
        | b.f_long,
        | a.dtEventTimeStamp
        | from test1 a
        | CROSS JOIN test2 b
      """.stripMargin)

    //  LEFT SEMI JOIN
    sqlAnalyzer(
      """
        |select
        |a.thedate,
        |a.dtEventTimeStamp
        |from test1 a
        |LEFT SEMI JOIN test2 b ON (a.thedate = b.thedate)
      """.stripMargin)

    //  全外关联（FULL [OUTER] JOIN）
    sqlAnalyzer(
      """
        |select
        |a.thedate,
        |a.dtEventTimeStamp
        |from test1 a
        |FULL OUTER JOIN test2 b ON (a.thedate = b.thedate)
      """.stripMargin)

  }

   def getTables(): Seq[CatalogTable] = {
     val tables = new ListBuffer[CatalogTable]()
     tables.append(BkSparkSqlAnalyzer.createTableDesc(
       "test1",
       new StructType()
         .add("thedate", StringType)
         .add("dtEventTime", StringType)
         .add("dtEventTimeStamp", LongType)
         .add("localTime", StringType)
         .add("f_int", IntegerType)
         .add("f_long", LongType)
         .add("f_float", FloatType)
         .add("f_double", DoubleType)
     )
     )
     tables.append(BkSparkSqlAnalyzer.createTableDesc(
       "test2",
       new StructType()
         .add("thedate", StringType)
         .add("dtEventTime", StringType)
         .add("dtEventTimeStamp", LongType)
         .add("localTime", StringType)
         .add("f_int", IntegerType)
         .add("f_long", LongType)
         .add("f_float", FloatType)
         .add("f_double", DoubleType)
     )
     )
     tables.append(BkSparkSqlAnalyzer.createTableDesc(
       "test3",
       new StructType()
         .add("thedate", StringType)
         .add("dtEventTime", StringType)
         .add("dtEventTimeStamp", LongType)
         .add("localTime", StringType)
         .add("f_int", IntegerType)
         .add("f_long", LongType)
         .add("f_float", FloatType)
         .add("f_double", DoubleType)
     )
     )

     tables.append(BkSparkSqlAnalyzer.createTableDesc(
       "test4",
       new StructType()
         .add("a", StringType)
         .add("b", StringType)
         .add("c", LongType)
         .add("d", StringType)
         .add("e", IntegerType)
         .add("f", LongType)
         .add("g", FloatType)
         .add("h", DoubleType)
     )
     )

     tables.append(BkSparkSqlAnalyzer.createTableDesc(
       "user_device_info",
       new StructType()
         .add("device_id", StringType)
         .add("from_id", StringType)
         .add("ins_date", LongType)
         .add("log_date", StringType)
         .add("e", IntegerType)
         .add("f", LongType)
         .add("g", FloatType)
         .add("h", DoubleType)
     )
     )


     tables.append(BkSparkSqlAnalyzer.createTableDesc(
       "3g_device_id",
       new StructType()
         .add("device_id", StringType)
         .add("from_id", StringType)
         .add("ins_date", LongType)
         .add("log_date", StringType)
         .add("e", IntegerType)
         .add("f", LongType)
         .add("g", FloatType)
         .add("h", DoubleType)
     )
     )

     tables.toSeq
   }



  private def sqlAnalyzer(sql: String, tables: Seq[CatalogTable] = getTables()): Unit = {
    val start = System.currentTimeMillis()
    val qe = BkSparkSqlAnalyzer.parserSQL(sql, tables, BkSparkSqlAnalyzer.getFunctionRegistry())
    // scalastyle:off println
    println(qe.schema.fields.mkString(", "))
    qe.printSchema()
    println("use time:" + (System.currentTimeMillis() - start))
  }
}

