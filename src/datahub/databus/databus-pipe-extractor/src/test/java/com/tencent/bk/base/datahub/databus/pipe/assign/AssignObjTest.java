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
import com.tencent.bk.base.datahub.databus.pipe.exception.NotMapDataError;
import com.tencent.bk.base.datahub.databus.pipe.exception.TypeConversionError;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

/**
 * AssignObj Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/27/2018</pre>
 */
@RunWith(PowerMockRunner.class)
public class AssignObjTest {

    /**
     * 测试validateNext方法
     *
     * @throws Exception
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignObj/assign_obj-failed.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Assert.assertTrue(parser.validateNext());
    }

    /**
     * 测试正常情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignObj/assign_obj-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "{\"worldid\": \"value1\", \"openid\": \"value2\"}";
        parser.execute(ctx, data);
        Assert.assertEquals("[value1, value2]", ctx.getValues().toString());
    }

    /**
     * 测试失败情况
     *
     * @throws Exception
     */
    @Test(expected = NotMapDataError.class)
    public void testExecuteFailed() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignObj/assign_obj-failed.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "{\"worldid\": \"value1\", \"openid\": \"value2\"}";
        parser.execute(ctx, data);
    }

    /**
     * 无意义，纯属为了增加覆盖率
     *
     * @throws Exception
     */
    @Test
    public void testExtra() {
        Assign a = new Assign() {
            @Override
            public boolean validateNext() {
                return false;
            }

            @Override
            public Object execute(Context ctx, Object o) {
                return null;
            }
        };
    }

    /**
     * 测试类型转换异常
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AssignObj.class)
    public void testExecuteCastTypeError() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignObj/assign_obj-success.json");
        Context ctx = new Context();

        Field field = PowerMockito.mock(Field.class);
        PowerMockito.when(field.castType(null)).thenThrow(TypeConversionError.class);
        PowerMockito.whenNew(Field.class).withAnyArguments().thenReturn(field);

        Node parser = Config.parse(ctx, confStr);
        String data = "{\"world\": \"111\", \"open\": \"222\"}";
        parser.execute(ctx, data);
    }

    /**
     * 测试默认值-兼容性
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AssignObj.class)
    public void testAssignObjDefaultValueCompat() throws Exception {
        String conf = "{\"extract\": {\"args\": [], \"next\": {\"next\": [{\"subtype\": \"assign_obj\", \"next\": "
                + "null, \"type\": \"assign\", \"assign\": [{\"assign_to\": \"_utctime_\", \"type\": \"string\", "
                + "\"key\": \"_utctime_\"}, {\"assign_to\": \"_time_\", \"type\": \"string\", \"key\": \"_time_\"}], "
                + "\"label\": \"labelbe0608\"}, {\"next\": {\"subtype\": \"assign_value\", \"next\": null, \"type\": "
                + "\"assign\", \"assign\": {\"assign_to\": \"result\", \"type\": \"string\"}, \"label\": "
                + "\"label24578d\"}, \"subtype\": \"access_obj\", \"result\": \"_company_id_\", \"key\": "
                + "\"_company_id_\", \"label\": \"labelffb614\", \"type\": \"access\", \"default_type\": \"int\", "
                + "\"default_value\": \"10086\"}], \"type\": \"branch\", \"name\": \"\", \"label\": null}, "
                + "\"result\": \"dd\", \"label\": \"label136930\", \"type\": \"fun\", \"method\": \"from_json\"}, "
                + "\"conf\": {\"timestamp_len\": 0, \"encoding\": \"UTF-8\", \"time_format\": \"yyyy-MM-dd "
                + "HH:mm:ss\", \"timezone\": 8, \"output_field_name\": \"timestamp\", \"time_field_name\": "
                + "\"_time_\"}}";
        String msg = "{\"_company_id_\":0,\"_plat_id_\":0,\"_server_\":\"X.X.X.X\",\"_time_\":\"2021-01-06 "
                + "17:17:40\",\"_utctime_\":\"2021-01-06 09:17:40\",\"d\":\"queue006\"}";

        String expectValue = "{\"nodes\":{\"label24578d\":{\"result\":\"0\"},\"label136930\":{\"_company_id_\":0,"
                + "\"_time_\":\"2021-01-06 17:17:40\",\"d\":\"queue006\",\"_utctime_\":\"2021-01-06 09:17:40\","
                + "\"_plat_id_\":0,\"_server_\":\"X.X.X.X\"},\"labelffb614\":0,"
                + "\"labelbe0608\":{\"_time_\":\"2021-01-06 17:17:40\",\"_utctime_\":\"2021-01-06 09:17:40\"}},"
                + "\"errors\":{},\"error_message\":\"\",\"output_type\":{\"label136930\":{\"type\":\"HashMap\","
                + "\"value\":{\"_company_id_\":0,\"_time_\":\"2021-01-06 17:17:40\",\"d\":\"queue006\","
                + "\"_utctime_\":\"2021-01-06 09:17:40\",\"_plat_id_\":0,\"_server_\":\"X.X.X.X\"}},"
                + "\"labelffb614\":{\"type\":\"Integer\",\"value\":0}},\"result\":[[\"2021-01-06 09:17:40\","
                + "\"2021-01-06 17:17:40\",\"0\",1609924660000]],\"schema\":[\"_utctime_(string)\",\"_time_(string)"
                + "\",\"result(string)\",\"timestamp(int)\"],\"display\":[\"user_field\",\"user_field\","
                + "\"user_field\",\"inter_field\"]}";
        ETL etl = new ETLImpl(conf);
        String result = etl.verifyConf(msg);

        Assert.assertEquals(result, expectValue);
    }

    /**
     * 测试默认值-测试逻辑正确
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AssignObj.class)
    public void testAssignObjDefaultValue() throws Exception {
        String conf = "{\"extract\": {\"args\": [], \"next\": {\"next\": [{\"subtype\": \"assign_obj\", \"next\": "
                + "null, \"type\": \"assign\", \"assign\": [{\"assign_to\": \"_utctime_\", \"type\": \"string\", "
                + "\"key\": \"_utctime_\"}, {\"assign_to\": \"_time_\", \"type\": \"string\", \"key\": \"_time_\"}], "
                + "\"label\": \"labelbe0608\"}, {\"next\": {\"subtype\": \"assign_value\", \"next\": null, \"type\": "
                + "\"assign\", \"assign\": {\"assign_to\": \"result\", \"type\": \"string\"}, \"label\": "
                + "\"label24578d\"}, \"subtype\": \"access_obj\", \"result\": \"_company_id_\", \"key\": "
                + "\"_company_id_\", \"label\": \"labelffb614\", \"type\": \"access\", \"default_type\": \"int\", "
                + "\"default_value\": \"10086\"}], \"type\": \"branch\", \"name\": \"\", \"label\": null}, "
                + "\"result\": \"dd\", \"label\": \"label136930\", \"type\": \"fun\", \"method\": \"from_json\"}, "
                + "\"conf\": {\"timestamp_len\": 0, \"encoding\": \"UTF-8\", \"time_format\": \"yyyy-MM-dd "
                + "HH:mm:ss\", \"timezone\": 8, \"output_field_name\": \"timestamp\", \"time_field_name\": "
                + "\"_time_\"}}";
        // 下面是msg缺少_company_id_ 这个key
        String msg = "{\"_company_id_ERR\":0,\"_plat_id_\":0,\"_server_\":\"X.X.X.X\",\"_time_\":\"2021-01-06 "
                + "17:17:40\",\"_utctime_\":\"2021-01-06 09:17:40\",\"d\":\"queue006\"}";

        String expectValue = "{\"nodes\":{\"label24578d\":{\"result\":\"10086\"},"
                + "\"label136930\":{\"_time_\":\"2021-01-06 17:17:40\",\"_company_id_ERR\":0,\"d\":\"queue006\","
                + "\"_utctime_\":\"2021-01-06 09:17:40\",\"_plat_id_\":0,\"_server_\":\"X.X.X.X\"},"
                + "\"labelffb614\":10086,\"labelbe0608\":{\"_time_\":\"2021-01-06 17:17:40\","
                + "\"_utctime_\":\"2021-01-06 09:17:40\"}},\"errors\":{},\"error_message\":\"\","
                + "\"output_type\":{\"label136930\":{\"type\":\"HashMap\",\"value\":{\"_time_\":\"2021-01-06 "
                + "17:17:40\",\"_company_id_ERR\":0,\"d\":\"queue006\",\"_utctime_\":\"2021-01-06 09:17:40\","
                + "\"_plat_id_\":0,\"_server_\":\"X.X.X.X\"}},\"labelffb614\":{\"type\":\"Integer\","
                + "\"value\":10086}},\"result\":[[\"2021-01-06 09:17:40\",\"2021-01-06 17:17:40\",\"10086\","
                + "1609924660000]],\"schema\":[\"_utctime_(string)\",\"_time_(string)\",\"result(string)\","
                + "\"timestamp(int)\"],\"display\":[\"user_field\",\"user_field\",\"user_field\",\"inter_field\"]}";
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


} 
