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

package com.tencent.bk.base.datalab.queryengine.server.parser;

import com.tencent.bk.base.datalab.bksql.deparser.DeParser;
import com.tencent.bk.base.datalab.bksql.deparser.Sql;
import com.tencent.bk.base.datalab.bksql.parser.calcite.SqlParserFactory;
import com.tencent.bk.base.datalab.bksql.protocol.BaseProtocolPlugin;
import com.typesafe.config.ConfigFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

public class ParseTest {

    private static DeParser deParser = new Sql(ConfigFactory.empty(),
            BaseProtocolPlugin.EMPTY_WALKER_CONFIG);

    /**
     * 复杂子查询 + 聚合查询 + EXISTS
     */
    public static void test1() throws Exception {
        String sql = "select  \n"
                + "  cd_gender,\n"
                + "  cd_marital_status,\n"
                + "  cd_education_status,\n"
                + "  count(*) cnt1,\n"
                + "  cd_purchase_estimate,\n"
                + "  count(*) cnt2,\n"
                + "  cd_credit_rating,\n"
                + "  count(*) cnt3,\n"
                + "  cd_dep_count,\n"
                + "  count(*) cnt4,\n"
                + "  cd_dep_employed_count,\n"
                + "  count(*) cnt5,\n"
                + "  cd_dep_college_count,\n"
                + "  count(*) cnt6\n"
                + " from\n"
                + "  customer c,customer_address ca,customer_demographics\n"
                + " where\n"
                + "  c.c_current_addr_sk = ca.ca_address_sk and\n"
                + "  ca_county in ('Clinton County','Platte County','Franklin County','Louisa "
                + "County','Harmon County') and\n"
                + "  cd_demo_sk = c.c_current_cdemo_sk and \n"
                + "  exists (select *\n"
                + "          from store_sales,date_dim\n"
                + "          where c.c_customer_sk = ss_customer_sk and\n"
                + "                ss_sold_date_sk = d_date_sk and\n"
                + "                d_year = 2002 and\n"
                + "                d_moy between 3 and 3+3) and\n"
                + "   (exists (select *\n"
                + "            from web_sales,date_dim\n"
                + "            where c.c_customer_sk = ws_bill_customer_sk and\n"
                + "                  ws_sold_date_sk = d_date_sk and\n"
                + "                  d_year = 2002 and\n"
                + "                  d_moy between 3 ANd 3+3) or \n"
                + "    exists (select * \n"
                + "            from catalog_sales,date_dim\n"
                + "            where c.c_customer_sk = cs_ship_customer_sk and\n"
                + "                  cs_sold_date_sk = d_date_sk and\n"
                + "                  d_year = 2002 and\n"
                + "                  d_moy between 3 and 3+3))\n"
                + " group by cd_gender,\n"
                + "          cd_marital_status,\n"
                + "          cd_education_status,\n"
                + "          cd_purchase_estimate,\n"
                + "          cd_credit_rating,\n"
                + "          cd_dep_count,\n"
                + "          cd_dep_employed_count,\n"
                + "          cd_dep_college_count\n"
                + " order by cd_gender,\n"
                + "          cd_marital_status,\n"
                + "          cd_education_status,\n"
                + "          cd_purchase_estimate,\n"
                + "          cd_credit_rating,\n"
                + "          cd_dep_count,\n"
                + "          cd_dep_employed_count,\n"
                + "          cd_dep_college_count\n"
                + "limit 100";
        SqlParser sqlParser = SqlParserFactory.getInstance().createParser(sql);
        SqlNode parsed = sqlParser.parseStmt();
        deParser.deParse(parsed);
    }

    /**
     * 多个JOIN查询
     */
    public static void test2() throws Exception {
        String sql = "select  i_item_desc\n"
                + "      ,w_warehouse_name\n"
                + "      ,d1.d_week_seq\n"
                + "      ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo\n"
                + "      ,sum(case when p_promo_sk is not null then 1 else 0 end) promo\n"
                + "      ,count(*) total_cnt\n"
                + "from catalog_sales\n"
                + "join inventory on (cs_item_sk = inv_item_sk)\n"
                + "join warehouse on (w_warehouse_sk=inv_warehouse_sk)\n"
                + "join item on (i_item_sk = cs_item_sk)\n"
                + "join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)\n"
                + "join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)\n"
                + "join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)\n"
                + "join date_dim d2 on (inv_date_sk = d2.d_date_sk)\n"
                + "join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)\n"
                + "left outer join promotion on (cs_promo_sk=p_promo_sk)\n"
                + "left outer join catalog_returns on (cr_item_sk = cs_item_sk and "
                + "cr_order_number = cs_order_number)\n"
                + "where d1.d_week_seq = d2.d_week_seq\n"
                + "  and inv_quantity_on_hand < cs_quantity \n"
                + "  and d3.d_date > d1.d_date + 5\n"
                + "  and hd_buy_potential = '501-1000'\n"
                + "  and d1.d_year = 1999\n"
                + "  and cd_marital_status = 'S'\n"
                + "group by i_item_desc,w_warehouse_name,d1.d_week_seq\n"
                + "order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq\n"
                + "limit 100";
        SqlParser sqlParser = SqlParserFactory.getInstance().createParser(sql);
        SqlNode parsed = sqlParser.parseQuery();
        deParser.deParse(parsed);
    }

    public static void main(String[] args) throws Exception {
        long loopTimes = 1000;
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < loopTimes; i++) {
            test1();
        }
        System.out.printf("test1() run [%d] times cost total time:[%d] ms \r\n", loopTimes,
                (System.currentTimeMillis() - t1));

        long t2 = System.currentTimeMillis();
        for (int i = 0; i < loopTimes; i++) {
            test2();
        }
        System.out.printf("test2() run [%d] times cost total time:[%d] ms \r\n", loopTimes,
                (System.currentTimeMillis() - t2));

    }

}
