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

package com.tencent.bk.base.datalab.bksql.deparser;

import com.tencent.bk.base.datalab.bksql.protocol.BaseProtocolPlugin;
import com.typesafe.config.ConfigFactory;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class TpchDsTest extends DeParserTestSupport {


    /**
     * -- sql 1 简单聚合查询
     */
    @Test
    public void testSql1() throws Exception {
        String sql = "select  i_item_id, \n"
                + "        avg(ss_quantity) agg1,\n"
                + "        avg(ss_list_price) agg2,\n"
                + "        avg(ss_coupon_amt) agg3,\n"
                + "        avg(ss_sales_price) agg4 \n"
                + " from store_sales, customer_demographics, date_dim, item, promotion\n"
                + " where ss_sold_date_sk = d_date_sk and\n"
                + "       ss_item_sk = i_item_sk and\n"
                + "       ss_cdemo_sk = cd_demo_sk and\n"
                + "       ss_promo_sk = p_promo_sk and\n"
                + "       cd_gender = 'M' and \n"
                + "       cd_marital_status = 'M' and\n"
                + "       cd_education_status = '4 yr Degree' and\n"
                + "       (p_channel_email = 'N' or p_channel_event = 'N') and\n"
                + "       d_year = 2001 \n"
                + " group by i_item_id\n"
                + " order by i_item_id\n"
                + " limit 100";
        String expected = "SELECT i_item_id, avg(ss_quantity) AS agg1, avg(ss_list_price) AS "
                + "agg2, avg(ss_coupon_amt) AS agg3, avg(ss_sales_price) AS agg4 FROM "
                + "store_sales, customer_demographics, date_dim, item, promotion WHERE ((((((("
                + "(ss_sold_date_sk = d_date_sk) AND (ss_item_sk = i_item_sk)) AND (ss_cdemo_sk ="
                + " cd_demo_sk)) AND (ss_promo_sk = p_promo_sk)) AND (cd_gender = 'M')) AND "
                + "(cd_marital_status = 'M')) AND (cd_education_status = '4 yr Degree')) AND ("
                + "(p_channel_email = 'N') OR (p_channel_event = 'N'))) AND (d_year = 2001) GROUP"
                + " BY i_item_id ORDER BY i_item_id LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * --  sql 2 复杂聚合查询
     */
    @Test
    public void testSql2() throws Exception {
        String sql = "select  i_item_id\n"
                + "       ,i_item_desc\n"
                + "       ,s_state\n"
                + "       ,count(ss_quantity) as store_sales_quantitycount\n"
                + "       ,avg(ss_quantity) as store_sales_quantityave\n"
                + "       ,stddev_samp(ss_quantity) as store_sales_quantitystdev\n"
                + "       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov\n"
                + "       ,count(sr_return_quantity) as store_returns_quantitycount\n"
                + "       ,avg(sr_return_quantity) as store_returns_quantityave\n"
                + "       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev\n"
                + "       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as "
                + "store_returns_quantitycov\n"
                + "       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as"
                + " catalog_sales_quantityave\n"
                + "       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev\n"
                + "       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov\n"
                + " from store_sales\n"
                + "     ,store_returns\n"
                + "     ,catalog_sales\n"
                + "     ,date_dim d1\n"
                + "     ,date_dim d2\n"
                + "     ,date_dim d3\n"
                + "     ,store\n"
                + "     ,item\n"
                + " where d1.d_quarter_name = '1999Q1'\n"
                + "   and d1.d_date_sk = ss_sold_date_sk\n"
                + "   and i_item_sk = ss_item_sk\n"
                + "   and s_store_sk = ss_store_sk\n"
                + "   and ss_customer_sk = sr_customer_sk\n"
                + "   and ss_item_sk = sr_item_sk\n"
                + "   and ss_ticket_number = sr_ticket_number\n"
                + "   and sr_returned_date_sk = d2.d_date_sk\n"
                + "   and d2.d_quarter_name in ('1999Q1','1999Q2','1999Q3')\n"
                + "   and sr_customer_sk = cs_bill_customer_sk\n"
                + "   and sr_item_sk = cs_item_sk\n"
                + "   and cs_sold_date_sk = d3.d_date_sk\n"
                + "   and d3.d_quarter_name in ('1999Q1','1999Q2','1999Q3')\n"
                + " group by i_item_id\n"
                + "         ,i_item_desc\n"
                + "         ,s_state\n"
                + " order by i_item_id\n"
                + "         ,i_item_desc\n"
                + "         ,s_state\n"
                + "limit 100";
        String expected = "SELECT i_item_id, i_item_desc, s_state, count(ss_quantity) AS "
                + "store_sales_quantitycount, avg(ss_quantity) AS store_sales_quantityave, "
                + "stddev_samp(ss_quantity) AS store_sales_quantitystdev, ((stddev_samp"
                + "(ss_quantity)) / (avg(ss_quantity))) AS store_sales_quantitycov, count"
                + "(sr_return_quantity) AS store_returns_quantitycount, avg(sr_return_quantity) "
                + "AS store_returns_quantityave, stddev_samp(sr_return_quantity) AS "
                + "store_returns_quantitystdev, ((stddev_samp(sr_return_quantity)) / (avg"
                + "(sr_return_quantity))) AS store_returns_quantitycov, count(cs_quantity) AS "
                + "catalog_sales_quantitycount, avg(cs_quantity) AS catalog_sales_quantityave, "
                + "stddev_samp(cs_quantity) AS catalog_sales_quantitystdev, ((stddev_samp"
                + "(cs_quantity)) / (avg(cs_quantity))) AS catalog_sales_quantitycov FROM "
                + "store_sales, store_returns, catalog_sales, date_dim AS d1, date_dim AS d2, "
                + "date_dim AS d3, store, item WHERE ((((((((((((d1.d_quarter_name = '1999Q1') "
                + "AND (d1.d_date_sk = ss_sold_date_sk)) AND (i_item_sk = ss_item_sk)) AND "
                + "(s_store_sk = ss_store_sk)) AND (ss_customer_sk = sr_customer_sk)) AND "
                + "(ss_item_sk = sr_item_sk)) AND (ss_ticket_number = sr_ticket_number)) AND "
                + "(sr_returned_date_sk = d2.d_date_sk)) AND (d2.d_quarter_name IN ('1999Q1', "
                + "'1999Q2', '1999Q3'))) AND (sr_customer_sk = cs_bill_customer_sk)) AND "
                + "(sr_item_sk = cs_item_sk)) AND (cs_sold_date_sk = d3.d_date_sk)) AND (d3"
                + ".d_quarter_name IN ('1999Q1', '1999Q2', '1999Q3')) GROUP BY i_item_id, "
                + "i_item_desc, s_state ORDER BY i_item_id, i_item_desc, s_state LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 3 简单聚合查询 + 复杂WHERE条件 + CASE WHEN
     */
    @Test
    public void testSql3() throws Exception {
        String sql = "select  *\n"
                + " from(select w_warehouse_name\n"
                + "            ,i_item_id\n"
                + "            ,sum(case when (cast(d_date as date) < cast ('2000-05-19' as date)"
                + ")\n"
                + "\t                then inv_quantity_on_hand \n"
                + "                      else 0 end) as inv_before\n"
                + "            ,sum(case when (cast(d_date as date) >= cast ('2000-05-19' as "
                + "date))\n"
                + "                      then inv_quantity_on_hand \n"
                + "                      else 0 end) as inv_after\n"
                + "   from inventory\n"
                + "       ,warehouse\n"
                + "       ,item\n"
                + "       ,date_dim\n"
                + "   where i_current_price between 0.99 and 1.49\n"
                + "     and i_item_sk          = inv_item_sk\n"
                + "     and inv_warehouse_sk   = w_warehouse_sk\n"
                + "     and inv_date_sk    = d_date_sk\n"
                + "     and d_date between (cast ('2000-05-19' as date) - interval '30' day)\n"
                + "                    and (cast ('2000-05-19' as date) + interval '30' day)\n"
                + "   group by w_warehouse_name, i_item_id) x\n"
                + " where (case when inv_before > 0 \n"
                + "             then inv_after / inv_before \n"
                + "             else null\n"
                + "             end) between 2.0/3.0 and 3.0/2.0\n"
                + " order by w_warehouse_name\n"
                + "         ,i_item_id\n"
                + " limit 100";
        String expected = "SELECT * FROM (SELECT w_warehouse_name, i_item_id, sum(CASE WHEN (CAST"
                + "(d_date AS DATE)) < (CAST('2000-05-19' AS DATE)) THEN inv_quantity_on_hand "
                + "ELSE 0 END) AS inv_before, sum(CASE WHEN (CAST(d_date AS DATE)) >= (CAST"
                + "('2000-05-19' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END) AS inv_after "
                + "FROM inventory, warehouse, item, date_dim WHERE ((((i_current_price BETWEEN 0"
                + ".99 AND 1.49) AND (i_item_sk = inv_item_sk)) AND (inv_warehouse_sk = "
                + "w_warehouse_sk)) AND (inv_date_sk = d_date_sk)) AND (d_date BETWEEN (CAST"
                + "('2000-05-19' AS DATE)) - INTERVAL '30' DAY AND (CAST('2000-05-19' AS DATE)) +"
                + " INTERVAL '30' DAY) GROUP BY w_warehouse_name, i_item_id) AS x WHERE CASE WHEN"
                + " inv_before > 0 THEN (inv_after / inv_before) ELSE null END BETWEEN 2.0 / 3.0 "
                + "AND 3.0 / 2.0 ORDER BY w_warehouse_name, i_item_id LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 4 简单子查询
     */
    @Test
    public void testSql4() throws Exception {
        String sql = "SELECT SUM(cs_ext_discount_amt) AS excess_discount_amount\n"
                + "FROM catalog_sales, item, date_dim\n"
                + "WHERE (i_manufact_id = 283\n"
                + "\tAND i_item_sk = cs_item_sk\n"
                + "\tAND d_date BETWEEN '1999-02-22' AND CAST('1999-02-22' AS date) + INTERVAL "
                + "'90' DAY\n"
                + "\tAND d_date_sk = cs_sold_date_sk\n"
                + "\tAND cs_ext_discount_amt > (\n"
                + "\t\tSELECT 1.3 * AVG(cs_ext_discount_amt)\n"
                + "\t\tFROM catalog_sales, date_dim\n"
                + "\t\tWHERE (cs_item_sk = i_item_sk\n"
                + "\t\t\tAND d_date BETWEEN '1999-02-22' AND CAST('1999-02-22' AS date) + "
                + "INTERVAL '90' DAY\n"
                + "\t\t\tAND d_date_sk = cs_sold_date_sk)\n"
                + "\t))\n"
                + "LIMIT 100";
        String expected = "SELECT SUM(cs_ext_discount_amt) AS excess_discount_amount FROM "
                + "catalog_sales, item, date_dim WHERE ((((i_manufact_id = 283) AND (i_item_sk = "
                + "cs_item_sk)) AND (d_date BETWEEN '1999-02-22' AND (CAST('1999-02-22' AS DATE))"
                + " + INTERVAL '90' DAY)) AND (d_date_sk = cs_sold_date_sk)) AND "
                + "(cs_ext_discount_amt > (SELECT 1.3 * (AVG(cs_ext_discount_amt)) FROM "
                + "catalog_sales, date_dim WHERE ((cs_item_sk = i_item_sk) AND (d_date BETWEEN "
                + "'1999-02-22' AND (CAST('1999-02-22' AS DATE)) + INTERVAL '90' DAY)) AND "
                + "(d_date_sk = cs_sold_date_sk))) LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 5 复杂子查询 + 聚合查询 + EXISTS
     */
    @Test
    public void testSql5() throws Exception {
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
        String expected = "SELECT cd_gender, cd_marital_status, cd_education_status, count(*) AS "
                + "cnt1, cd_purchase_estimate, count(*) AS cnt2, cd_credit_rating, count(*) AS "
                + "cnt3, cd_dep_count, count(*) AS cnt4, cd_dep_employed_count, count(*) AS cnt5,"
                + " cd_dep_college_count, count(*) AS cnt6 FROM customer AS c, customer_address "
                + "AS ca, customer_demographics WHERE ((((c.c_current_addr_sk = ca.ca_address_sk)"
                + " AND (ca_county IN ('Clinton County', 'Platte County', 'Franklin County', "
                + "'Louisa County', 'Harmon County'))) AND (cd_demo_sk = c.c_current_cdemo_sk)) "
                + "AND (EXISTS ((SELECT * FROM store_sales, date_dim WHERE (((c.c_customer_sk = "
                + "ss_customer_sk) AND (ss_sold_date_sk = d_date_sk)) AND (d_year = 2002)) AND "
                + "(d_moy BETWEEN 3 AND 3 + 3))))) AND ((EXISTS ((SELECT * FROM web_sales, "
                + "date_dim WHERE (((c.c_customer_sk = ws_bill_customer_sk) AND (ws_sold_date_sk "
                + "= d_date_sk)) AND (d_year = 2002)) AND (d_moy BETWEEN 3 AND 3 + 3)))) OR "
                + "(EXISTS ((SELECT * FROM catalog_sales, date_dim WHERE (((c.c_customer_sk = "
                + "cs_ship_customer_sk) AND (cs_sold_date_sk = d_date_sk)) AND (d_year = 2002)) "
                + "AND (d_moy BETWEEN 3 AND 3 + 3))))) GROUP BY cd_gender, cd_marital_status, "
                + "cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, "
                + "cd_dep_employed_count, cd_dep_college_count ORDER BY cd_gender, "
                + "cd_marital_status, cd_education_status, cd_purchase_estimate, "
                + "cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count "
                + "LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 6 简单UNION + 子查询 + left join
     */
    @Test
    public void testSql6() throws Exception {
        String sql = " SELECT d_year\n"
                + "       ,i_brand_id\n"
                + "       ,i_class_id\n"
                + "       ,i_category_id\n"
                + "       ,i_manufact_id\n"
                + "       ,SUM(sales_cnt) AS sales_cnt\n"
                + "       ,SUM(sales_amt) AS sales_amt\n"
                + " FROM (SELECT d_year\n"
                + "             ,i_brand_id\n"
                + "             ,i_class_id\n"
                + "             ,i_category_id\n"
                + "             ,i_manufact_id\n"
                + "             ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt\n"
                + "             ,cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt\n"
                + "       FROM catalog_sales JOIN item ON i_item_sk=cs_item_sk\n"
                + "                          JOIN date_dim ON d_date_sk=cs_sold_date_sk\n"
                + "                          LEFT JOIN catalog_returns ON "
                + "(cs_order_number=cr_order_number \n"
                + "                                                    AND cs_item_sk=cr_item_sk)\n"
                + "       WHERE i_category='Shoes'\n"
                + "       UNION\n"
                + "       SELECT d_year\n"
                + "             ,i_brand_id\n"
                + "             ,i_class_id\n"
                + "             ,i_category_id\n"
                + "             ,i_manufact_id\n"
                + "             ,ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt\n"
                + "             ,ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt\n"
                + "       FROM store_sales JOIN item ON i_item_sk=ss_item_sk\n"
                + "                        JOIN date_dim ON d_date_sk=ss_sold_date_sk\n"
                + "                        LEFT JOIN store_returns ON "
                + "(ss_ticket_number=sr_ticket_number \n"
                + "                                                AND ss_item_sk=sr_item_sk)\n"
                + "       WHERE i_category='Shoes'\n"
                + "       UNION\n"
                + "       SELECT d_year\n"
                + "             ,i_brand_id\n"
                + "             ,i_class_id\n"
                + "             ,i_category_id\n"
                + "             ,i_manufact_id\n"
                + "             ,ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt\n"
                + "             ,ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt\n"
                + "       FROM web_sales JOIN item ON i_item_sk=ws_item_sk\n"
                + "                      JOIN date_dim ON d_date_sk=ws_sold_date_sk\n"
                + "                      LEFT JOIN web_returns ON "
                + "(ws_order_number=wr_order_number \n"
                + "                                            AND ws_item_sk=wr_item_sk)\n"
                + "       WHERE i_category='Shoes') sales_detail\n"
                + " GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id";
        String expected = "SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, "
                + "SUM(sales_cnt) AS sales_cnt, SUM(sales_amt) AS sales_amt FROM (((SELECT "
                + "d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, (cs_quantity - "
                + "(COALESCE(cr_return_quantity, 0))) AS sales_cnt, (cs_ext_sales_price - "
                + "(COALESCE(cr_return_amount, 0.0))) AS sales_amt FROM catalog_sales INNER JOIN "
                + "item ON (i_item_sk = cs_item_sk) INNER JOIN date_dim ON (d_date_sk = "
                + "cs_sold_date_sk) LEFT OUTER JOIN catalog_returns ON ((cs_order_number = "
                + "cr_order_number) AND (cs_item_sk = cr_item_sk)) WHERE i_category = 'Shoes') "
                + "UNION (SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, "
                + "(ss_quantity - (COALESCE(sr_return_quantity, 0))) AS sales_cnt, "
                + "(ss_ext_sales_price - (COALESCE(sr_return_amt, 0.0))) AS sales_amt FROM "
                + "store_sales INNER JOIN item ON (i_item_sk = ss_item_sk) INNER JOIN date_dim ON"
                + " (d_date_sk = ss_sold_date_sk) LEFT OUTER JOIN store_returns ON ("
                + "(ss_ticket_number = sr_ticket_number) AND (ss_item_sk = sr_item_sk)) WHERE "
                + "i_category = 'Shoes')) UNION (SELECT d_year, i_brand_id, i_class_id, "
                + "i_category_id, i_manufact_id, (ws_quantity - (COALESCE(wr_return_quantity, 0))"
                + ") AS sales_cnt, (ws_ext_sales_price - (COALESCE(wr_return_amt, 0.0))) AS "
                + "sales_amt FROM web_sales INNER JOIN item ON (i_item_sk = ws_item_sk) INNER "
                + "JOIN date_dim ON (d_date_sk = ws_sold_date_sk) LEFT OUTER JOIN web_returns ON "
                + "((ws_order_number = wr_order_number) AND (ws_item_sk = wr_item_sk)) WHERE "
                + "i_category = 'Shoes')) AS sales_detail GROUP BY d_year, i_brand_id, "
                + "i_class_id, i_category_id, i_manufact_id";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 7 简单UNION ALL查询
     */
    @Test
    public void testSql7() throws Exception {
        String sql =
                " select  channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM"
                        + "(ext_sales_price) sales_amt FROM (\n"
                        + "        SELECT 'store' as channel, 'ss_customer_sk' col_name, d_year, "
                        + "d_qoy, i_category, ss_ext_sales_price ext_sales_price\n"
                        + "         FROM store_sales, item, date_dim\n"
                        + "         WHERE ss_customer_sk IS NULL\n"
                        + "           AND ss_sold_date_sk=d_date_sk\n"
                        + "           AND ss_item_sk=i_item_sk\n"
                        + "        UNION ALL\n"
                        + "        SELECT 'web' as channel, 'ws_ship_hdemo_sk' col_name, d_year, "
                        + "d_qoy, i_category, ws_ext_sales_price ext_sales_price\n"
                        + "         FROM web_sales, item, date_dim\n"
                        + "         WHERE ws_ship_hdemo_sk IS NULL\n"
                        + "           AND ws_sold_date_sk=d_date_sk\n"
                        + "           AND ws_item_sk=i_item_sk\n"
                        + "        UNION ALL\n"
                        + "        SELECT 'catalog' as channel, 'cs_bill_customer_sk' col_name, "
                        + "d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price\n"
                        + "         FROM catalog_sales, item, date_dim\n"
                        + "         WHERE cs_bill_customer_sk IS NULL\n"
                        + "           AND cs_sold_date_sk=d_date_sk\n"
                        + "           AND cs_item_sk=i_item_sk) foo\n"
                        + "GROUP BY channel, col_name, d_year, d_qoy, i_category\n"
                        + "ORDER BY channel, col_name, d_year, d_qoy, i_category\n"
                        + "limit 100";
        String expected = "SELECT channel, col_name, d_year, d_qoy, i_category, COUNT(*) AS "
                + "sales_cnt, SUM(ext_sales_price) AS sales_amt FROM (((SELECT 'store' AS "
                + "channel, 'ss_customer_sk' AS col_name, d_year, d_qoy, i_category, "
                + "ss_ext_sales_price AS ext_sales_price FROM store_sales, item, date_dim WHERE ("
                + "(ss_customer_sk IS NULL) AND (ss_sold_date_sk = d_date_sk)) AND (ss_item_sk = "
                + "i_item_sk)) UNION ALL (SELECT 'web' AS channel, 'ws_ship_hdemo_sk' AS "
                + "col_name, d_year, d_qoy, i_category, ws_ext_sales_price AS ext_sales_price "
                + "FROM web_sales, item, date_dim WHERE ((ws_ship_hdemo_sk IS NULL) AND "
                + "(ws_sold_date_sk = d_date_sk)) AND (ws_item_sk = i_item_sk))) UNION ALL "
                + "(SELECT 'catalog' AS channel, 'cs_bill_customer_sk' AS col_name, d_year, "
                + "d_qoy, i_category, cs_ext_sales_price AS ext_sales_price FROM catalog_sales, "
                + "item, date_dim WHERE ((cs_bill_customer_sk IS NULL) AND (cs_sold_date_sk = "
                + "d_date_sk)) AND (cs_item_sk = i_item_sk))) AS foo GROUP BY channel, col_name, "
                + "d_year, d_qoy, i_category ORDER BY channel, col_name, d_year, d_qoy, "
                + "i_category LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 8 复杂union all + 子查询 + 聚合查询
     */
    @Test
    public void testSql8() throws Exception {
        String sql = "\n"
                + " select s_store_id,\n"
                + "        sum(sales_price) as sales,\n"
                + "        sum(profit) as profit,\n"
                + "        sum(return_amt) as sum_return_amt,\n"
                + "        sum(net_loss) as profit_loss\n"
                + " from\n"
                + "  ( select  ss_store_sk as store_sk,\n"
                + "            ss_sold_date_sk  as date_sk,\n"
                + "            ss_ext_sales_price as sales_price,\n"
                + "            ss_net_profit as profit,\n"
                + "            cast(0 as decimal(7,2)) as return_amt,\n"
                + "            cast(0 as decimal(7,2)) as net_loss\n"
                + "    from store_sales\n"
                + "    union all\n"
                + "    select sr_store_sk as store_sk,\n"
                + "           sr_returned_date_sk as date_sk,\n"
                + "           cast(0 as decimal(7,2)) as sales_price,\n"
                + "           cast(0 as decimal(7,2)) as profit,\n"
                + "           sr_return_amt as return_amt,\n"
                + "           sr_net_loss as net_loss\n"
                + "    from store_returns\n"
                + "   ) salesreturns,\n"
                + "     date_dim,\n"
                + "     store\n"
                + " where date_sk = d_date_sk\n"
                + "       and d_date between cast('2001-08-04' as date) \n"
                + "                  and (cast('2001-08-04' as date) + interval '14' day)\n"
                + "       and store_sk = s_store_sk\n"
                + " group by s_store_id";
        String expected = "SELECT s_store_id, sum(sales_price) AS sales, sum(profit) AS profit, "
                + "sum(return_amt) AS sum_return_amt, sum(net_loss) AS profit_loss FROM ((SELECT "
                + "ss_store_sk AS store_sk, ss_sold_date_sk AS date_sk, ss_ext_sales_price AS "
                + "sales_price, ss_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, "
                + "CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM store_sales) UNION ALL (SELECT "
                + "sr_store_sk AS store_sk, sr_returned_date_sk AS date_sk, CAST(0 AS DECIMAL(7, "
                + "2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, sr_return_amt AS "
                + "return_amt, sr_net_loss AS net_loss FROM store_returns)) AS salesreturns, "
                + "date_dim, store WHERE ((date_sk = d_date_sk) AND (d_date BETWEEN CAST"
                + "('2001-08-04' AS DATE) AND (CAST('2001-08-04' AS DATE)) + INTERVAL '14' DAY)) "
                + "AND (store_sk = s_store_sk) GROUP BY s_store_id";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 9 复杂分析函数(rank over, coalesce)查询 + union
     */
    @Test
    public void testSql9() throws Exception {
        String sql = "select channel, item, return_ratio, return_rank, currency_rank from(select "
                + "'web' as channel ,web.item ,web.return_ratio ,web.return_rank ,web"
                + ".currency_rank from (select item,return_ratio,currency_ratio,rank() over "
                + "(order by return_ratio) as return_rank,rank() over (order by currency_ratio) "
                + "as currency_rank from(select ws.ws_item_sk as item,(cast(sum(coalesce(wr"
                + ".wr_return_quantity,0)) as decimal(15,4))/cast(sum(coalesce(ws.ws_quantity,0))"
                + " as decimal(15,4))) as return_ratio,(cast(sum(coalesce(wr.wr_return_amt,0)) as"
                + " decimal(15,4))/cast(sum(coalesce(ws.ws_net_paid,0)) as decimal(15,4) )) as "
                + "currency_ratio from web_sales ws left outer join web_returns wr on (ws"
                + ".ws_order_number = wr.wr_order_number and ws.ws_item_sk = wr.wr_item_sk) ,"
                + "date_dim where wr.wr_return_amt > 10000 and ws.ws_net_profit > 1 and ws"
                + ".ws_net_paid > 0 and ws.ws_quantity > 0 and ws_sold_date_sk = d_date_sk and "
                + "d_year = 2000 and d_moy = 12 group by ws.ws_item_sk) in_web) web where (web"
                + ".return_rank <= 10 or web.currency_rank <= 10 ) union select 'catalog' as "
                + "channel ,catalog.item ,catalog.return_ratio ,catalog.return_rank ,catalog"
                + ".currency_rank from ( select item ,return_ratio ,currency_ratio ,rank() over "
                + "(order by return_ratio) as return_rank ,rank() over (order by currency_ratio) "
                + "as currency_rank from ( select cs.cs_item_sk as item ,(cast(sum(coalesce(cr"
                + ".cr_return_quantity,0)) as decimal(15,4))/ cast(sum(coalesce(cs.cs_quantity,0)"
                + ") as decimal(15,4) )) as return_ratio ,(cast(sum(coalesce(cr.cr_return_amount,"
                + "0)) as decimal(15,4))/ cast(sum(coalesce(cs.cs_net_paid,0)) as decimal(15,4) )"
                + ") as currency_ratio from catalog_sales cs left outer join catalog_returns cr "
                + "on (cs.cs_order_number = cr.cr_order_number and cs.cs_item_sk = cr.cr_item_sk)"
                + " ,date_dim where cr.cr_return_amount > 10000 and cs.cs_net_profit > 1 and cs"
                + ".cs_net_paid > 0 and cs.cs_quantity > 0 and cs_sold_date_sk = d_date_sk and "
                + "d_year = 2000 and d_moy = 12 group by cs.cs_item_sk ) in_cat ) catalog where ("
                + " catalog.return_rank <= 10 or catalog.currency_rank <=10 ) union select "
                + "'store' as channel ,store.item ,store.return_ratio ,store.return_rank ,store"
                + ".currency_rank from ( select item ,return_ratio ,currency_ratio ,rank() over "
                + "(order by return_ratio) as return_rank ,rank() over (order by currency_ratio) "
                + "as currency_rank from ( select sts.ss_item_sk as item ,(cast(sum(coalesce(sr"
                + ".sr_return_quantity,0)) as decimal(15,4))/cast(sum(coalesce(sts.ss_quantity,0)"
                + ") as decimal(15,4) )) as return_ratio ,(cast(sum(coalesce(sr.sr_return_amt,0))"
                + " as decimal(15,4))/cast(sum(coalesce(sts.ss_net_paid,0)) as decimal(15,4) )) "
                + "as currency_ratio from store_sales sts left outer join store_returns sr on "
                + "(sts.ss_ticket_number = sr.sr_ticket_number and sts.ss_item_sk = sr"
                + ".sr_item_sk) ,date_dim where sr.sr_return_amt > 10000 and sts.ss_net_profit > "
                + "1 and sts.ss_net_paid > 0 and sts.ss_quantity > 0 and ss_sold_date_sk = "
                + "d_date_sk and d_year = 2000 and d_moy = 12 group by sts.ss_item_sk ) in_store "
                + ") store where ( store.return_rank <= 10 or store.currency_rank <= 10 ) ) order"
                + " by 1,4,5,2 limit 100";
        String expected = "SELECT channel, item, return_ratio, return_rank, currency_rank FROM ("
                + "(SELECT 'web' AS channel, web.item, web.return_ratio, web.return_rank, web"
                + ".currency_rank FROM (SELECT item, return_ratio, currency_ratio, (rank() OVER"
                + "(ORDER BY return_ratio)) AS return_rank, (rank() OVER(ORDER BY currency_ratio)"
                + ") AS currency_rank FROM (SELECT ws.ws_item_sk AS item, ((CAST(sum(coalesce(wr"
                + ".wr_return_quantity, 0)) AS DECIMAL(15, 4))) / (CAST(sum(coalesce(ws"
                + ".ws_quantity, 0)) AS DECIMAL(15, 4)))) AS return_ratio, ((CAST(sum(coalesce(wr"
                + ".wr_return_amt, 0)) AS DECIMAL(15, 4))) / (CAST(sum(coalesce(ws.ws_net_paid, "
                + "0)) AS DECIMAL(15, 4)))) AS currency_ratio FROM web_sales AS ws LEFT OUTER "
                + "JOIN web_returns AS wr ON ((ws.ws_order_number = wr.wr_order_number) AND (ws"
                + ".ws_item_sk = wr.wr_item_sk)), date_dim WHERE ((((((wr.wr_return_amt > 10000) "
                + "AND (ws.ws_net_profit > 1)) AND (ws.ws_net_paid > 0)) AND (ws.ws_quantity > 0)"
                + ") AND (ws_sold_date_sk = d_date_sk)) AND (d_year = 2000)) AND (d_moy = 12) "
                + "GROUP BY ws.ws_item_sk) AS in_web) AS web WHERE (web.return_rank <= 10) OR "
                + "(web.currency_rank <= 10)) UNION (SELECT 'catalog' AS channel, catalog.item, "
                + "catalog.return_ratio, catalog.return_rank, catalog.currency_rank FROM (SELECT "
                + "item, return_ratio, currency_ratio, (rank() OVER(ORDER BY return_ratio)) AS "
                + "return_rank, (rank() OVER(ORDER BY currency_ratio)) AS currency_rank FROM "
                + "(SELECT cs.cs_item_sk AS item, ((CAST(sum(coalesce(cr.cr_return_quantity, 0)) "
                + "AS DECIMAL(15, 4))) / (CAST(sum(coalesce(cs.cs_quantity, 0)) AS DECIMAL(15, 4)"
                + "))) AS return_ratio, ((CAST(sum(coalesce(cr.cr_return_amount, 0)) AS DECIMAL"
                + "(15, 4))) / (CAST(sum(coalesce(cs.cs_net_paid, 0)) AS DECIMAL(15, 4)))) AS "
                + "currency_ratio FROM catalog_sales AS cs LEFT OUTER JOIN catalog_returns AS cr "
                + "ON ((cs.cs_order_number = cr.cr_order_number) AND (cs.cs_item_sk = cr"
                + ".cr_item_sk)), date_dim WHERE ((((((cr.cr_return_amount > 10000) AND (cs"
                + ".cs_net_profit > 1)) AND (cs.cs_net_paid > 0)) AND (cs.cs_quantity > 0)) AND "
                + "(cs_sold_date_sk = d_date_sk)) AND (d_year = 2000)) AND (d_moy = 12) GROUP BY "
                + "cs.cs_item_sk) AS in_cat) AS catalog WHERE (catalog.return_rank <= 10) OR "
                + "(catalog.currency_rank <= 10))) UNION (SELECT 'store' AS channel, store.item, "
                + "store.return_ratio, store.return_rank, store.currency_rank FROM (SELECT item, "
                + "return_ratio, currency_ratio, (rank() OVER(ORDER BY return_ratio)) AS "
                + "return_rank, (rank() OVER(ORDER BY currency_ratio)) AS currency_rank FROM "
                + "(SELECT sts.ss_item_sk AS item, ((CAST(sum(coalesce(sr.sr_return_quantity, 0))"
                + " AS DECIMAL(15, 4))) / (CAST(sum(coalesce(sts.ss_quantity, 0)) AS DECIMAL(15, "
                + "4)))) AS return_ratio, ((CAST(sum(coalesce(sr.sr_return_amt, 0)) AS DECIMAL"
                + "(15, 4))) / (CAST(sum(coalesce(sts.ss_net_paid, 0)) AS DECIMAL(15, 4)))) AS "
                + "currency_ratio FROM store_sales AS sts LEFT OUTER JOIN store_returns AS sr ON "
                + "((sts.ss_ticket_number = sr.sr_ticket_number) AND (sts.ss_item_sk = sr"
                + ".sr_item_sk)), date_dim WHERE ((((((sr.sr_return_amt > 10000) AND (sts"
                + ".ss_net_profit > 1)) AND (sts.ss_net_paid > 0)) AND (sts.ss_quantity > 0)) AND"
                + " (ss_sold_date_sk = d_date_sk)) AND (d_year = 2000)) AND (d_moy = 12) GROUP BY"
                + " sts.ss_item_sk) AS in_store) AS store WHERE (store.return_rank <= 10) OR "
                + "(store.currency_rank <= 10)) ORDER BY 1, 4, 5, 2 LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 10 复杂case when查询
     */
    @Test
    public void testSql10() throws Exception {
        String sql = "select case when (select count(*) \n"
                + "                  from store_sales \n"
                + "                  where ss_quantity between 1 and 20) > 31002\n"
                + "            then (select avg(ss_ext_discount_amt) \n"
                + "                  from store_sales \n"
                + "                  where ss_quantity between 1 and 20) \n"
                + "            else (select avg(ss_net_profit)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 1 and 20) end bucket1 ,\n"
                + "       case when (select count(*)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 21 and 40) > 588\n"
                + "            then (select avg(ss_ext_discount_amt)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 21 and 40) \n"
                + "            else (select avg(ss_net_profit)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 21 and 40) end bucket2,\n"
                + "       case when (select count(*)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 41 and 60) > 2456\n"
                + "            then (select avg(ss_ext_discount_amt)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 41 and 60)\n"
                + "            else (select avg(ss_net_profit)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 41 and 60) end bucket3,\n"
                + "       case when (select count(*)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 61 and 80) > 21645\n"
                + "            then (select avg(ss_ext_discount_amt)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 61 and 80)\n"
                + "            else (select avg(ss_net_profit)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 61 and 80) end bucket4,\n"
                + "       case when (select count(*)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 81 and 100) > 20553\n"
                + "            then (select avg(ss_ext_discount_amt)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 81 and 100)\n"
                + "            else (select avg(ss_net_profit)\n"
                + "                  from store_sales\n"
                + "                  where ss_quantity between 81 and 100) end bucket5\n"
                + "from reason\n"
                + "where r_reason_sk = 1";
        String expected = "SELECT CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity "
                + "BETWEEN 1 AND 20) > 31002 THEN (SELECT avg(ss_ext_discount_amt) FROM "
                + "store_sales WHERE ss_quantity BETWEEN 1 AND 20) ELSE (SELECT avg"
                + "(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) END AS "
                + "bucket1, CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN"
                + " 21 AND 40) > 588 THEN (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE"
                + " ss_quantity BETWEEN 21 AND 40) ELSE (SELECT avg(ss_net_profit) FROM "
                + "store_sales WHERE ss_quantity BETWEEN 21 AND 40) END AS bucket2, CASE WHEN "
                + "(SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60) > 2456 "
                + "THEN (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity "
                + "BETWEEN 41 AND 60) ELSE (SELECT avg(ss_net_profit) FROM store_sales WHERE "
                + "ss_quantity BETWEEN 41 AND 60) END AS bucket3, CASE WHEN (SELECT count(*) FROM"
                + " store_sales WHERE ss_quantity BETWEEN 61 AND 80) > 21645 THEN (SELECT avg"
                + "(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80) "
                + "ELSE (SELECT avg(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 61 "
                + "AND 80) END AS bucket4, CASE WHEN (SELECT count(*) FROM store_sales WHERE "
                + "ss_quantity BETWEEN 81 AND 100) > 20553 THEN (SELECT avg(ss_ext_discount_amt) "
                + "FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) ELSE (SELECT avg"
                + "(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) END AS "
                + "bucket5 FROM reason WHERE r_reason_sk = 1";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 12 多个JOIN查询
     */
    @Test
    public void testSql11() throws Exception {
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
        String expected = "SELECT i_item_desc, w_warehouse_name, d1.d_week_seq, sum(CASE WHEN "
                + "p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo, sum(CASE WHEN p_promo_sk IS"
                + " NOT NULL THEN 1 ELSE 0 END) AS promo, count(*) AS total_cnt FROM "
                + "catalog_sales INNER JOIN inventory ON (cs_item_sk = inv_item_sk) INNER JOIN "
                + "warehouse ON (w_warehouse_sk = inv_warehouse_sk) INNER JOIN item ON (i_item_sk"
                + " = cs_item_sk) INNER JOIN customer_demographics ON (cs_bill_cdemo_sk = "
                + "cd_demo_sk) INNER JOIN household_demographics ON (cs_bill_hdemo_sk = "
                + "hd_demo_sk) INNER JOIN date_dim AS d1 ON (cs_sold_date_sk = d1.d_date_sk) "
                + "INNER JOIN date_dim AS d2 ON (inv_date_sk = d2.d_date_sk) INNER JOIN date_dim "
                + "AS d3 ON (cs_ship_date_sk = d3.d_date_sk) LEFT OUTER JOIN promotion ON "
                + "(cs_promo_sk = p_promo_sk) LEFT OUTER JOIN catalog_returns ON ((cr_item_sk = "
                + "cs_item_sk) AND (cr_order_number = cs_order_number)) WHERE (((((d1.d_week_seq "
                + "= d2.d_week_seq) AND (inv_quantity_on_hand < cs_quantity)) AND (d3.d_date > "
                + "(d1.d_date + 5))) AND (hd_buy_potential = '501-1000')) AND (d1.d_year = 1999))"
                + " AND (cd_marital_status = 'S') GROUP BY i_item_desc, w_warehouse_name, d1"
                + ".d_week_seq ORDER BY total_cnt DESC, i_item_desc, w_warehouse_name, d_week_seq"
                + " LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    /**
     * -- sql 12 FULL OUTER JOIN查询
     */
    @Test
    public void testSql12() throws Exception {
        String sql =
                "select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null "
                        + "then 1 else 0 end) store_only\n"
                        + "      ,sum(case when ssci.customer_sk is null and csci.customer_sk is "
                        + "not null then 1 else 0 end) catalog_only\n"
                        + "      ,sum(case when ssci.customer_sk is not null and csci.customer_sk"
                        + " is not null then 1 else 0 end) store_and_catalog\n"
                        + "from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk\n"
                        + "                               and ssci.item_sk = csci.item_sk)\n"
                        + "limit 100";
        String expected = "SELECT sum(CASE WHEN (ssci.customer_sk IS NOT NULL) AND (csci"
                + ".customer_sk IS NULL) THEN 1 ELSE 0 END) AS store_only, sum(CASE WHEN (ssci"
                + ".customer_sk IS NULL) AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END) AS"
                + " catalog_only, sum(CASE WHEN (ssci.customer_sk IS NOT NULL) AND (csci"
                + ".customer_sk IS NOT NULL) THEN 1 ELSE 0 END) AS store_and_catalog FROM ssci "
                + "FULL OUTER JOIN csci ON ((ssci.customer_sk = csci.customer_sk) AND (ssci"
                + ".item_sk = csci.item_sk)) LIMIT 100";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }


}
