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

package com.tencent.bk.base.datahub.databus.pipe.assign;

import com.tencent.bk.base.datahub.databus.pipe.Config;
import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import com.tencent.bk.base.datahub.databus.pipe.exception.NotListDataError;
import com.tencent.bk.base.datahub.databus.pipe.exception.TypeConversionError;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AssignPos Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/27/2018</pre>
 */
@RunWith(PowerMockRunner.class)
public class AssignPosTest {

    /**
     * 测试validateNext方法
     *
     * @throws Exception
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignPos/assign_pos-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Assert.assertTrue(parser.validateNext());
    }

    /**
     * 测试传入数据是List的成功情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteListDataSuccess() throws Exception {
        List<String> listData = new ArrayList<>();
        listData.add("value1");
        listData.add("value2");
        String confStr = TestUtils.getFileContent("/assign/AssignPos/assign_pos-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, listData);
        Assert.assertEquals("[value1, value2]", ctx.getValues().toString());
    }

    /**
     * 测试传入数据不是List的成功情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteListNotDataSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignPos/assign_pos_data_not_list.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "helloWorld";
        parser.execute(ctx, data);
        Assert.assertEquals("helloWorld", ctx.getValues().get(0).toString());
    }

    /**
     * 测试传入数据是Map的失败情况
     *
     * @throws Exception
     */
    @Test(expected = NotListDataError.class)
    public void testExecuteNotListDataError() throws Exception {
        Map<String, String> mapData = new HashMap();
        mapData.put("openid", "123");
        String confStr = TestUtils.getFileContent("/assign/AssignPos/assign_pos-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, mapData);
    }

    /**
     * 测试传入数据是List的失败情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AssignPos.class)
    public void testExecuteListDataFailed() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignPos/assign_pos-success.json");
        Context ctx = new Context();
        List<String> listData = new ArrayList<>();
        listData.add("value1");
        listData.add("value2");

        Field field = PowerMockito.mock(Field.class);
        PowerMockito.when(field.castType("value1")).thenThrow(TypeConversionError.class);
        PowerMockito.whenNew(Field.class).withAnyArguments().thenReturn(field);

        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, listData);
    }

    /**
     * 测试index out of bounds失败情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AssignPos.class)
    public void testMissingFieldInsideIterator() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignPos/assign_pos-success.json");
        Context ctx = new Context();
        List<String> listData = new ArrayList<>();
        listData.add("value1");
        listData.add("value2");

        Field field = PowerMockito.mock(Field.class);
        PowerMockito.when(field.castType("value1")).thenThrow(IndexOutOfBoundsException.class);
        PowerMockito.whenNew(Field.class).withAnyArguments().thenReturn(field);

        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, listData);
    }

    /**
     * 测试类型转换失败的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AssignPos.class)
    public void testExecuteCastTypeError() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignPos/assign_pos-success.json");
        Context ctx = new Context();

        Field field = PowerMockito.mock(Field.class);
        String data = "test TypeConversionError";
        PowerMockito.when(field.castType(data)).thenThrow(TypeConversionError.class);
        PowerMockito.whenNew(Field.class).withAnyArguments().thenReturn(field);

        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, data);
        Assert.assertNotEquals(0, ctx.badValues.size());
    }

    /**
     * 测试默认值-兼容性
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AssignObj.class)
    public void testDefaultValueCompat() throws Exception {
        String conf = "{\"extract\": {\"args\": [], \"next\": {\"next\": [{\"subtype\": \"assign_obj\", \"next\": "
                + "null, \"type\": \"assign\", \"assign\": [{\"assign_to\": \"_utctime_\", \"type\": \"string\", "
                + "\"key\": \"_utctime_\"}, {\"assign_to\": \"d\", \"type\": \"string\", \"key\": \"d\"}, "
                + "{\"assign_to\": \"_company_id_\", \"type\": \"string\", \"key\": \"_company_id_\"}, "
                + "{\"assign_to\": \"_server_\", \"type\": \"string\", \"key\": \"_server_\"}, {\"assign_to\": "
                + "\"_plat_id_\", \"type\": \"string\", \"key\": \"_plat_id_\"}, {\"assign_to\": \"_time_\", "
                + "\"type\": \"string\", \"key\": \"_time_\"}], \"label\": \"label24578d\"}, {\"next\": {\"index\": "
                + "0, \"next\": {\"subtype\": \"assign_value\", \"next\": null, \"type\": \"assign\", \"assign\": "
                + "{\"assign_to\": \"index_test\", \"type\": \"string\"}, \"label\": \"label0a46f9\"}, \"subtype\": "
                + "\"access_pos\", \"result\": \"index_test\", \"label\": \"label692f77\", \"type\": \"access\", "
                + "\"default_type\": \"int\", \"default_value\": \"10010\"}, \"subtype\": \"access_obj\", \"result\":"
                + " \"_company_id_\", \"key\": \"_company_id_\", \"label\": \"labelffb614\", \"type\": \"access\"}], "
                + "\"type\": \"branch\", \"name\": \"\", \"label\": null}, \"result\": \"dd\", \"label\": "
                + "\"label136930\", \"type\": \"fun\", \"method\": \"from_json\"}, \"conf\": {\"timestamp_len\": 0, "
                + "\"encoding\": \"UTF-8\", \"time_format\": \"yyyy-MM-dd HH:mm:ss\", \"timezone\": 8, "
                + "\"output_field_name\": \"timestamp\", \"time_field_name\": \"_utctime_\"}}";
        String msg = "{\"_company_id_\":[1,2,3],\"_plat_id_\":0,\"_server_\":\"X.X.X.X\",\"_time_\":\"2021-01-06 "
                + "17:17:40\",\"_utctime_\":\"2021-01-06 09:17:40\",\"d\":\"queue006\"}";

        String expectValue = "{\"nodes\":{\"label692f77\":1,\"label24578d\":{\"_company_id_\":\"[1, 2, 3]\","
                + "\"_time_\":\"2021-01-06 17:17:40\",\"d\":\"queue006\",\"_utctime_\":\"2021-01-06 09:17:40\","
                + "\"_plat_id_\":\"0\",\"_server_\":\"X.X.X.X\"},\"label136930\":{\"_company_id_\":[1,2,3],"
                + "\"_time_\":\"2021-01-06 17:17:40\",\"d\":\"queue006\",\"_utctime_\":\"2021-01-06 09:17:40\","
                + "\"_plat_id_\":0,\"_server_\":\"X.X.X.X\"},\"labelffb614\":[1,2,3],"
                + "\"label0a46f9\":{\"index_test\":\"1\"}},\"errors\":{},\"error_message\":\"\","
                + "\"output_type\":{\"label692f77\":{\"type\":\"Integer\",\"value\":1},"
                + "\"label136930\":{\"type\":\"HashMap\",\"value\":{\"_company_id_\":[1,2,3],\"_time_\":\"2021-01-06 "
                + "17:17:40\",\"d\":\"queue006\",\"_utctime_\":\"2021-01-06 09:17:40\",\"_plat_id_\":0,"
                + "\"_server_\":\"X.X.X.X\"}},\"labelffb614\":{\"type\":\"ArrayList\",\"value\":[1,2,3]}},"
                + "\"result\":[[\"2021-01-06 09:17:40\",\"queue006\",\"[1, 2, 3]\",\"X.X.X.X\",\"0\",\"2021-01-06 "
                + "17:17:40\",\"1\",1609895860000]],\"schema\":[\"_utctime_(string)\",\"d(string)\",\"_company_id_"
                + "(string)\",\"_server_(string)\",\"_plat_id_(string)\",\"_time_(string)\",\"index_test(string)\","
                + "\"timestamp(int)\"],\"display\":[\"user_field\",\"user_field\",\"user_field\",\"user_field\","
                + "\"user_field\",\"user_field\",\"user_field\",\"inter_field\"]}";
        ETL etl = new ETLImpl(conf);
        String result = etl.verifyConf(msg);

        Assert.assertEquals(result, expectValue);
    }

    /**
     * 测试默认值-抛出异常
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AssignObj.class)
    public void testAssignPosDefaultValueNull() throws Exception {
        //  "default_type": "null", "default_value": ""
        String conf = "{\"extract\": {\"args\": [], \"next\": {\"next\": [{\"subtype\": \"assign_obj\", \"next\": "
                + "null, \"type\": \"assign\", \"assign\": [{\"assign_to\": \"_utctime_\", \"type\": \"string\", "
                + "\"key\": \"_utctime_\"}, {\"assign_to\": \"d\", \"type\": \"string\", \"key\": \"d\"}, "
                + "{\"assign_to\": \"_company_id_\", \"type\": \"string\", \"key\": \"_company_id_\"}, "
                + "{\"assign_to\": \"_server_\", \"type\": \"string\", \"key\": \"_server_\"}, {\"assign_to\": "
                + "\"_plat_id_\", \"type\": \"string\", \"key\": \"_plat_id_\"}, {\"assign_to\": \"_time_\", "
                + "\"type\": \"string\", \"key\": \"_time_\"}], \"label\": \"label24578d\"}, {\"next\": {\"index\": "
                + "999, \"next\": {\"subtype\": \"assign_value\", \"next\": null, \"type\": \"assign\", \"assign\": "
                + "{\"assign_to\": \"index_test\", \"type\": \"string\"}, \"label\": \"label0a46f9\"}, \"subtype\": "
                + "\"access_pos\", \"result\": \"index_test\", \"label\": \"label692f77\", \"type\": \"access\", "
                + "\"default_type\": \"null\", \"default_value\": \"\"}, \"subtype\": \"access_obj\", \"result\": "
                + "\"_company_id_\", \"key\": \"_company_id_\", \"label\": \"labelffb614\", \"type\": \"access\"}], "
                + "\"type\": \"branch\", \"name\": \"\", \"label\": null}, \"result\": \"dd\", \"label\": "
                + "\"label136930\", \"type\": \"fun\", \"method\": \"from_json\"}, \"conf\": {\"timestamp_len\": 0, "
                + "\"encoding\": \"UTF-8\", \"time_format\": \"yyyy-MM-dd HH:mm:ss\", \"timezone\": 8, "
                + "\"output_field_name\": \"timestamp\", \"time_field_name\": \"_utctime_\"}}";
        String msg = "{\"_company_id_\":[1,2,3],\"_plat_id_\":0,\"_server_\":\"X.X.X.X\",\"_time_\":\"2021-01-06 "
                + "17:17:40\",\"_utctime_\":\"2021-01-06 09:17:40\",\"d\":\"queue006\"}";

        String expectError = "AccessByIndexFailedError: 999";
        ETL etl = new ETLImpl(conf);
        String result = etl.verifyConf(msg);
        String errorMsg = (String) ((Map) JsonUtils.readMap(result).get("errors")).get("label692f77");
        Assert.assertEquals(errorMsg, expectError);
    }

    /**
     * 测试默认值-测试逻辑正确
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AssignObj.class)
    public void testAssignPosDefaultValue() throws Exception {
        // index 改为999
        String conf = "{\"extract\": {\"args\": [], \"next\": {\"next\": [{\"subtype\": \"assign_obj\", \"next\": "
                + "null, \"type\": \"assign\", \"assign\": [{\"assign_to\": \"_utctime_\", \"type\": \"string\", "
                + "\"key\": \"_utctime_\"}, {\"assign_to\": \"d\", \"type\": \"string\", \"key\": \"d\"}, "
                + "{\"assign_to\": \"_company_id_\", \"type\": \"string\", \"key\": \"_company_id_\"}, "
                + "{\"assign_to\": \"_server_\", \"type\": \"string\", \"key\": \"_server_\"}, {\"assign_to\": "
                + "\"_plat_id_\", \"type\": \"string\", \"key\": \"_plat_id_\"}, {\"assign_to\": \"_time_\", "
                + "\"type\": \"string\", \"key\": \"_time_\"}], \"label\": \"label24578d\"}, {\"next\": {\"index\": "
                + "999, \"next\": {\"subtype\": \"assign_value\", \"next\": null, \"type\": \"assign\", \"assign\": "
                + "{\"assign_to\": \"index_test\", \"type\": \"string\"}, \"label\": \"label0a46f9\"}, \"subtype\": "
                + "\"access_pos\", \"result\": \"index_test\", \"label\": \"label692f77\", \"type\": \"access\", "
                + "\"default_type\": \"int\", \"default_value\": \"10010\"}, \"subtype\": \"access_obj\", \"result\":"
                + " \"_company_id_\", \"key\": \"_company_id_\", \"label\": \"labelffb614\", \"type\": \"access\"}], "
                + "\"type\": \"branch\", \"name\": \"\", \"label\": null}, \"result\": \"dd\", \"label\": "
                + "\"label136930\", \"type\": \"fun\", \"method\": \"from_json\"}, \"conf\": {\"timestamp_len\": 0, "
                + "\"encoding\": \"UTF-8\", \"time_format\": \"yyyy-MM-dd HH:mm:ss\", \"timezone\": 8, "
                + "\"output_field_name\": \"timestamp\", \"time_field_name\": \"_utctime_\"}}";
        String msg = "{\"_company_id_\":[1,2,3],\"_plat_id_\":0,\"_server_\":\"X.X.X.X\",\"_time_\":\"2021-01-06 "
                + "17:17:40\",\"_utctime_\":\"2021-01-06 09:17:40\",\"d\":\"queue006\"}";

        // 默认值预期为10010
        String expectValue = "{\"nodes\":{\"label692f77\":10010,\"label24578d\":{\"_company_id_\":\"[1, 2, 3]\","
                + "\"_time_\":\"2021-01-06 17:17:40\",\"d\":\"queue006\",\"_utctime_\":\"2021-01-06 09:17:40\","
                + "\"_plat_id_\":\"0\",\"_server_\":\"X.X.X.X\"},\"label136930\":{\"_company_id_\":[1,2,3],"
                + "\"_time_\":\"2021-01-06 17:17:40\",\"d\":\"queue006\",\"_utctime_\":\"2021-01-06 09:17:40\","
                + "\"_plat_id_\":0,\"_server_\":\"X.X.X.X\"},\"labelffb614\":[1,2,3],"
                + "\"label0a46f9\":{\"index_test\":\"10010\"}},\"errors\":{},\"error_message\":\"\","
                + "\"output_type\":{\"label692f77\":{\"type\":\"Integer\",\"value\":10010},"
                + "\"label136930\":{\"type\":\"HashMap\",\"value\":{\"_company_id_\":[1,2,3],\"_time_\":\"2021-01-06 "
                + "17:17:40\",\"d\":\"queue006\",\"_utctime_\":\"2021-01-06 09:17:40\",\"_plat_id_\":0,"
                + "\"_server_\":\"X.X.X.X\"}},\"labelffb614\":{\"type\":\"ArrayList\",\"value\":[1,2,3]}},"
                + "\"result\":[[\"2021-01-06 09:17:40\",\"queue006\",\"[1, 2, 3]\",\"X.X.X.X\",\"0\",\"2021-01-06 "
                + "17:17:40\",\"10010\",1609895860000]],\"schema\":[\"_utctime_(string)\",\"d(string)\","
                + "\"_company_id_(string)\",\"_server_(string)\",\"_plat_id_(string)\",\"_time_(string)\","
                + "\"index_test(string)\",\"timestamp(int)\"],\"display\":[\"user_field\",\"user_field\","
                + "\"user_field\",\"user_field\",\"user_field\",\"user_field\",\"user_field\",\"inter_field\"]}";
        ETL etl = new ETLImpl(conf);
        String result = etl.verifyConf(msg);
        Assert.assertEquals(result, expectValue);
    }
} 
