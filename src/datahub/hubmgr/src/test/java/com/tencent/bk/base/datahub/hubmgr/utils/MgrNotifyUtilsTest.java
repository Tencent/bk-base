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

package com.tencent.bk.base.datahub.hubmgr.utils;

import com.tencent.bk.base.datahub.databus.commons.utils.NotifyUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest(NotifyUtils.class)
public class MgrNotifyUtilsTest {

    static class KafkaOffsetWorker {
        // KafkaOffsetWorker默认在可发送服务中
        public static void sendMessage() {
            MgrNotifyUtils.sendFatalAlert(KafkaOffsetWorker.class, "admin", "test message");
        }
    }

    static class WarpClass {
        public static void sendMessage() {
            MgrNotifyUtils.sendFatalAlert(WarpClass.class, "admin", "test message");
        }
    }

    @Test
    public void testMessageNotSend() {
        PowerMockito.mockStatic(NotifyUtils.class);
        WarpClass.sendMessage();
    }

    @Test
    public void testMessageSendSuccess() {
        PowerMockito.mockStatic(NotifyUtils.class);
        KafkaOffsetWorker.sendMessage();
    }

}

