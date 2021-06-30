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

package com.tencent.bk.base.datahub.databus.connect.common;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.bean.ClusterStatBean;
import com.tencent.bk.base.datahub.databus.commons.bean.EventBean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MainTest {

    private List<Integer> splitIntoSegements(int total, int segements) {
        List<Integer> result = new ArrayList<>(segements);
        if (total <= segements) {
            for (int i = 0; i < total; i++) {
                result.add(1); // 一个分区一个元素
            }
        } else {
            for (int i = 0; i < segements; i++) {
                int count = (total / segements) + (total % segements > i ? 1 : 0);
                result.add(i, count);
            }
        }

        return result;
    }


    // for test only...
    private static Map<String, String> getTestOutKafka() {
        Map<String, String> result = new HashMap<>();
        // another test

        result.put(BkConfig.RT_ID, "591_test");
        result.put(Consts.COLUMNS,
                "time=long,name=string,Desk1Stat=long,TotalLen=long,TotalFraps=long,ActionLen=long,ActionFraps=long,"
                        + "BlkNum=int,GameSec=int,AvgFreq=int,BigThanMs1=int,Fraps1=int,BigThanMs2=int,Fraps2=int,"
                        + "StateQFull=int,AcntInputQFull=int,QFullAcnt=int,MaxCmdSeqLess=int,MaxCmdSeqBig=int,"
                        + "ip=string,_path_=string,_server_=string,_worldid_=int,_gseindex_=long");
        result.put(Consts.MSG_SOURCE_TYPE, Consts.ETL);
        result.put(Consts.TOPICS, "test591");
        result.put(Consts.DATA_ID, "123456");
        result.put(Consts.ETL_CONF,
                "{\"conf\":{\"time_format\":\"yyyy-MM-dd HH:mm:ss.SSSSSS\",\"encoding\":\"UTF8\","
                        + "\"time_field_name\":\"time\",\"output_field_name\":\"timestamp\",\"timestamp_len\":0},"
                        + "\"extract\":{\"type\":\"fun\",\"args\":[],\"method\":\"from_json\","
                        + "\"next\":{\"next\":[{\"subtype\":\"access_obj\",\"key\":\"_value_\",\"type\":\"access\","
                        + "\"next\":{\"type\":\"fun\",\"method\":\"iterate\",\"next\":{\"type\":\"fun\","
                        + "\"method\":\"split\",\"next\":{\"type\":\"branch\",\"name\":\"#branch2\","
                        + "\"next\":[{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":0,"
                        + "\"next\":{\"type\":\"fun\",\"next\":{\"subtype\":\"access_pos\",\"type\":\"access\","
                        + "\"index\":1,\"next\":{\"type\":\"fun\",\"args\":[\"[\"],\"method\":\"split\","
                        + "\"next\":{\"assign\":[{\"index\":0,\"assign_to\":\"time\",\"type\":\"string\"}],"
                        + "\"subtype\":\"assign_pos\",\"type\":\"assign\"}}},\"method\":\"split\",\"args\":[\"[\"]}},"
                        + "{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":1,"
                        + "\"next\":{\"method\":\"split\",\"next\":{\"index\":1,\"next\":{\"type\":\"fun\","
                        + "\"next\":{\"assign\":[{\"type\":\"string\",\"assign_to\":\"name\",\"index\":0}],"
                        + "\"type\":\"assign\",\"subtype\":\"assign_pos\"},\"method\":\"split\",\"args\":[\"[\"]},"
                        + "\"subtype\":\"access_pos\",\"type\":\"access\"},\"args\":[\"[\"],\"type\":\"fun\"}},"
                        + "{\"next\":{\"type\":\"fun\",\"next\":{\"name\":\"#branch3\",\"type\":\"branch\","
                        + "\"next\":[{\"type\":\"access\",\"subtype\":\"access_pos\",\"next\":{\"method\":\"split\","
                        + "\"next\":{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":1,"
                        + "\"next\":{\"next\":{\"subtype\":\"assign_pos\",\"type\":\"assign\","
                        + "\"assign\":[{\"type\":\"int\",\"index\":0,\"assign_to\":\"Desk1Stat\"}]},"
                        + "\"method\":\"split\",\"args\":[\">\"],\"type\":\"fun\"}},\"args\":[\"<\"],"
                        + "\"type\":\"fun\"},\"index\":0},{\"next\":{\"method\":\"split\",\"next\":{\"index\":1,"
                        + "\"next\":{\"next\":{\"assign\":[{\"type\":\"long\",\"assign_to\":\"TotalLen\","
                        + "\"index\":0}],\"type\":\"assign\",\"subtype\":\"assign_pos\"},\"method\":\"split\","
                        + "\"args\":[\":\"],\"type\":\"fun\"},\"subtype\":\"access_pos\",\"type\":\"access\"},"
                        + "\"args\":[\":\"],\"type\":\"fun\"},\"index\":1,\"type\":\"access\","
                        + "\"subtype\":\"access_pos\"},{\"next\":{\"args\":[\":\"],\"next\":{\"index\":1,"
                        + "\"next\":{\"type\":\"fun\",\"next\":{\"assign\":[{\"type\":\"long\",\"index\":0,"
                        + "\"assign_to\":\"TotalFraps\"}],\"type\":\"assign\",\"subtype\":\"assign_pos\"},"
                        + "\"method\":\"split\",\"args\":[\">\"]},\"subtype\":\"access_pos\",\"type\":\"access\"},"
                        + "\"method\":\"split\",\"type\":\"fun\"},\"index\":2,\"type\":\"access\","
                        + "\"subtype\":\"access_pos\"},{\"next\":{\"type\":\"fun\","
                        + "\"next\":{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":1,"
                        + "\"next\":{\"type\":\"fun\",\"args\":[\":\"],\"method\":\"split\","
                        + "\"next\":{\"subtype\":\"assign_pos\",\"type\":\"assign\","
                        + "\"assign\":[{\"assign_to\":\"ActionLen\",\"index\":0,\"type\":\"long\"}]}}},"
                        + "\"method\":\"split\",\"args\":[\":\"]},\"index\":3,\"type\":\"access\","
                        + "\"subtype\":\"access_pos\"},{\"type\":\"access\",\"subtype\":\"access_pos\","
                        + "\"next\":{\"type\":\"fun\",\"args\":[\":\"],\"method\":\"split\","
                        + "\"next\":{\"next\":{\"type\":\"fun\",\"args\":[\">\"],\"method\":\"split\","
                        + "\"next\":{\"assign\":[{\"index\":0,\"assign_to\":\"ActionFraps\",\"type\":\"long\"}],"
                        + "\"subtype\":\"assign_pos\",\"type\":\"assign\"}},\"index\":1,\"type\":\"access\","
                        + "\"subtype\":\"access_pos\"}},\"index\":4},{\"index\":5,\"next\":{\"type\":\"fun\","
                        + "\"method\":\"split\",\"next\":{\"index\":1,\"next\":{\"type\":\"fun\","
                        + "\"next\":{\"assign\":[{\"type\":\"int\",\"assign_to\":\"PureSpareFraps\",\"index\":0}],"
                        + "\"type\":\"assign\",\"subtype\":\"assign_pos\"},\"method\":\"split\",\"args\":[\">\"]},"
                        + "\"subtype\":\"access_pos\",\"type\":\"access\"},\"args\":[\"<\"]},"
                        + "\"subtype\":\"access_pos\",\"type\":\"access\"},{\"type\":\"access\","
                        + "\"subtype\":\"access_pos\",\"next\":{\"type\":\"fun\",\"args\":[\"<\"],"
                        + "\"next\":{\"index\":1,\"next\":{\"type\":\"fun\",\"method\":\"split\","
                        + "\"next\":{\"subtype\":\"assign_pos\",\"type\":\"assign\",\"assign\":[{\"type\":\"int\","
                        + "\"index\":0,\"assign_to\":\"BlkNum\"}]},\"args\":[\">\"]},\"subtype\":\"access_pos\","
                        + "\"type\":\"access\"},\"method\":\"split\"},\"index\":6},{\"type\":\"access\","
                        + "\"subtype\":\"access_pos\",\"next\":{\"type\":\"fun\",\"args\":[\"<\"],"
                        + "\"next\":{\"type\":\"access\",\"subtype\":\"access_pos\",\"next\":{\"type\":\"fun\","
                        + "\"args\":[\">\"],\"next\":{\"assign\":[{\"index\":0,\"assign_to\":\"GameSec\","
                        + "\"type\":\"int\"}],\"subtype\":\"assign_pos\",\"type\":\"assign\"},\"method\":\"split\"},"
                        + "\"index\":1},\"method\":\"split\"},\"index\":7},{\"next\":{\"type\":\"fun\","
                        + "\"args\":[\"<\"],\"method\":\"split\",\"next\":{\"index\":1,"
                        + "\"next\":{\"method\":\"split\",\"next\":{\"assign\":[{\"type\":\"int\",\"index\":0,"
                        + "\"assign_to\":\"AvgFreq\"}],\"type\":\"assign\",\"subtype\":\"assign_pos\"},"
                        + "\"args\":[\">\"],\"type\":\"fun\"},\"subtype\":\"access_pos\",\"type\":\"access\"}},"
                        + "\"index\":8,\"type\":\"access\",\"subtype\":\"access_pos\"},{\"index\":9,"
                        + "\"next\":{\"type\":\"fun\",\"method\":\"split\",\"next\":{\"subtype\":\"access_pos\","
                        + "\"type\":\"access\",\"index\":1,\"next\":{\"method\":\"split\","
                        + "\"next\":{\"type\":\"assign\",\"subtype\":\"assign_pos\",\"assign\":[{\"type\":\"int\","
                        + "\"assign_to\":\"MaxFreq\",\"index\":0}]},\"args\":[\">\"],\"type\":\"fun\"}},"
                        + "\"args\":[\"<\"]},\"subtype\":\"access_pos\",\"type\":\"access\"},"
                        + "{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":10,\"next\":{\"type\":\"fun\","
                        + "\"args\":[\"-\"],\"method\":\"split\",\"next\":{\"name\":\"#branch4\",\"type\":\"branch\","
                        + "\"next\":[{\"next\":{\"method\":\"split\",\"next\":{\"index\":1,"
                        + "\"next\":{\"method\":\"split\",\"next\":{\"type\":\"assign\",\"subtype\":\"assign_pos\","
                        + "\"assign\":[{\"index\":0,\"assign_to\":\"BigThanMs1\",\"type\":\"int\"}]},"
                        + "\"args\":[\">\"],\"type\":\"fun\"},\"subtype\":\"access_pos\",\"type\":\"access\"},"
                        + "\"args\":[\"<\"],\"type\":\"fun\"},\"index\":0,\"type\":\"access\","
                        + "\"subtype\":\"access_pos\"},{\"next\":{\"type\":\"fun\",\"next\":{\"type\":\"access\","
                        + "\"subtype\":\"access_pos\",\"next\":{\"type\":\"fun\",\"args\":[\">\"],"
                        + "\"next\":{\"assign\":[{\"index\":0,\"assign_to\":\"Fraps1\",\"type\":\"int\"}],"
                        + "\"type\":\"assign\",\"subtype\":\"assign_pos\"},\"method\":\"split\"},\"index\":1},"
                        + "\"method\":\"split\",\"args\":[\"<\"]},\"index\":1,\"type\":\"access\","
                        + "\"subtype\":\"access_pos\"}]}}},{\"next\":{\"type\":\"fun\",\"args\":[\"-\"],"
                        + "\"method\":\"split\",\"next\":{\"next\":[{\"next\":{\"type\":\"fun\",\"method\":\"split\","
                        + "\"next\":{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":1,"
                        + "\"next\":{\"args\":[\">\"],\"next\":{\"subtype\":\"assign_pos\",\"type\":\"assign\","
                        + "\"assign\":[{\"assign_to\":\"BigThanMs2\",\"index\":0,\"type\":\"int\"}]},"
                        + "\"method\":\"split\",\"type\":\"fun\"}},\"args\":[\"<\"]},\"index\":0,\"type\":\"access\","
                        + "\"subtype\":\"access_pos\"},{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":1,"
                        + "\"next\":{\"next\":{\"next\":{\"method\":\"split\",\"next\":{\"type\":\"assign\","
                        + "\"subtype\":\"assign_pos\",\"assign\":[{\"type\":\"int\",\"assign_to\":\"Fraps2\","
                        + "\"index\":0}]},\"args\":[\">\"],\"type\":\"fun\"},\"index\":1,\"type\":\"access\","
                        + "\"subtype\":\"access_pos\"},\"method\":\"split\",\"args\":[\"<\"],\"type\":\"fun\"}}],"
                        + "\"name\":\"#branch5\",\"type\":\"branch\"}},\"index\":11,\"type\":\"access\","
                        + "\"subtype\":\"access_pos\"},{\"index\":12,\"next\":{\"method\":\"split\","
                        + "\"next\":{\"type\":\"access\",\"subtype\":\"access_pos\","
                        + "\"next\":{\"next\":{\"assign\":[{\"index\":0,\"assign_to\":\"StateQFull\","
                        + "\"type\":\"int\"}],\"type\":\"assign\",\"subtype\":\"assign_pos\"},\"method\":\"split\","
                        + "\"args\":[\">\"],\"type\":\"fun\"},\"index\":1},\"args\":[\"<\"],\"type\":\"fun\"},"
                        + "\"subtype\":\"access_pos\",\"type\":\"access\"},{\"type\":\"access\","
                        + "\"subtype\":\"access_pos\",\"next\":{\"type\":\"fun\",\"method\":\"split\","
                        + "\"next\":{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":1,"
                        + "\"next\":{\"args\":[\">\"],\"method\":\"split\",\"next\":{\"subtype\":\"assign_pos\","
                        + "\"type\":\"assign\",\"assign\":[{\"type\":\"int\",\"index\":0,"
                        + "\"assign_to\":\"AcntInputQFull\"}]},\"type\":\"fun\"}},\"args\":[\"<\"]},\"index\":13},"
                        + "{\"index\":14,\"next\":{\"method\":\"split\",\"next\":{\"subtype\":\"access_pos\","
                        + "\"type\":\"access\",\"index\":1,\"next\":{\"method\":\"split\","
                        + "\"next\":{\"assign\":[{\"index\":0,\"assign_to\":\"QFullAcnt\",\"type\":\"int\"}],"
                        + "\"subtype\":\"assign_pos\",\"type\":\"assign\"},\"args\":[\">\"],\"type\":\"fun\"}},"
                        + "\"args\":[\"<\"],\"type\":\"fun\"},\"subtype\":\"access_pos\",\"type\":\"access\"},"
                        + "{\"type\":\"access\",\"subtype\":\"access_pos\",\"next\":{\"args\":[\"<\"],"
                        + "\"method\":\"split\",\"next\":{\"type\":\"access\",\"subtype\":\"access_pos\","
                        + "\"next\":{\"type\":\"fun\",\"args\":[\">\"],\"method\":\"split\","
                        + "\"next\":{\"subtype\":\"assign_pos\",\"type\":\"assign\",\"assign\":[{\"type\":\"int\","
                        + "\"index\":0,\"assign_to\":\"MaxCmdSeqLess\"}]}},\"index\":1},\"type\":\"fun\"},"
                        + "\"index\":15},{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":16,"
                        + "\"next\":{\"type\":\"fun\",\"args\":[\"<\"],\"method\":\"split\","
                        + "\"next\":{\"subtype\":\"access_pos\",\"type\":\"access\",\"index\":1,"
                        + "\"next\":{\"args\":[\">\"],\"next\":{\"assign\":[{\"type\":\"int\",\"index\":0,"
                        + "\"assign_to\":\"MaxCmdSeqBig\"}],\"subtype\":\"assign_pos\",\"type\":\"assign\"},"
                        + "\"method\":\"split\",\"type\":\"fun\"}}}}]},\"method\":\"split\",\"args\":[\" \"]},"
                        + "\"index\":2,\"type\":\"access\",\"subtype\":\"access_pos\"}]},\"args\":[\"]\"]},"
                        + "\"args\":[]}},{\"subtype\":\"assign_obj\",\"type\":\"assign\","
                        + "\"assign\":[{\"assign_to\":\"ip\",\"key\":\"_server_\",\"type\":\"string\"}]}],"
                        + "\"type\":\"branch\",\"name\":\"#branch1\"}}}");

        result.put("cluster.bootstrap.servers", "xx.xx.xx.xx:9092");
        result.put("cluster.rest.port", "10666");

        result.put("consumer.bootstrap.servers", "xx.xx.xx.xx:9092");
        result.put("consumer.max.poll.records", "5");

        return result;
    }

    private static Map<String, String> getTestInnerKafka() {
        Map<String, String> result = new HashMap<>();

        result.put(BkConfig.RT_ID, "591_test");
        result.put(Consts.COLUMNS,
                "worldid=int,avg=double,dtEventTime=string,dtEventTimeStamp=string,localTime=string");
        result.put(Consts.MSG_SOURCE_TYPE, Consts.AVRO);
        result.put(Consts.TOPICS, "table_591_test");
        result.put(Consts.DATA_ID, "0");
        result.put(Consts.ETL_CONF, "{}");

        result.put("consumer.max.poll.records", "50");

        return result;
    }

    public static void main(String[] args) throws Exception {
        MainTest test = new MainTest();
        System.out.println(test.splitIntoSegements(3, 4));
        System.out.println(test.splitIntoSegements(4, 4));
        System.out.println(test.splitIntoSegements(5, 4));
        System.out.println(test.splitIntoSegements(6, 4));
        System.out.println(test.splitIntoSegements(7, 4));
        System.out.println(test.splitIntoSegements(8, 4));
        System.out.println(test.splitIntoSegements(9, 4));
        int start = 0;
        for (int num : test.splitIntoSegements(9, 4)) {
            System.out.println("start " + start + " end " + (start + num));
            start += num;
        }

        EventBean event = new EventBean("uid", "t", "mess", "message");
        ObjectMapper mapper = new ObjectMapper();
        String s = mapper.writeValueAsString(event);
        System.out.println(s);
        EventBean event2 = mapper.readValue(s, EventBean.class);
        System.out.println(event2);
        event2.setExtra("sadfsfsdfsdf");
        System.out.println(mapper.writeValueAsString(event2));

        ClusterStatBean bean = new ClusterStatBean(100, 13234, 2392, 233123231, 1222332, 1533123231);
        s = mapper.writeValueAsString(bean);
        System.out.println(s);
        System.out.println(bean);
        ClusterStatBean bean1 = mapper.readValue(s, ClusterStatBean.class);
        System.out.println(bean1);
    }
}
