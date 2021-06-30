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

package com.tencent.bk.base.datahub.databus.commons.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * KeyGen Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/14/2018</pre>
 */
public class KeyGenTest {

    private String key = "250xxxxxxxxxxxxxxxxxxx==";
    private String rootKey="test_key00000000";
    private String keyIV ="0000000000000000";

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testConstructor() {
        KeyGen keyGen = new KeyGen();
        assertNotNull(keyGen);
    }

    /**
     * Method: encrypt(byte[] plainText, String instanceKey)
     */
    @Test
    public void testEncrypt() throws Exception {
        String line = "test msg";
        String encryptLine = KeyGen.encrypt(line.getBytes(StandardCharsets.UTF_8),rootKey,  keyIV, key);
        assertNotNull(encryptLine);
    }

    /**
     * Method: decrypt(String cipherText, String instanceKey)
     */
    @Test
    public void testDecrypt() throws Exception {
        String line = "test msg";
        String encryptLine = KeyGen.encrypt(line.getBytes(StandardCharsets.UTF_8),rootKey,  keyIV, key);
        byte[] result = KeyGen.decrypt(encryptLine,rootKey,  keyIV, key);
        assertEquals(line, new String(result, StandardCharsets.UTF_8));
    }


    /**
     * Method: parseInstanceKey(String instanceKey)
     */
    @Test
    public void testParseInstanceKey() throws Exception {

    }


}
