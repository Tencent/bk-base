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

package com.tencent.bk.base.datalab.queryengine.server.inject;

import com.beust.jcommander.JCommander;
import com.tencent.bk.base.datalab.queryengine.common.numeric.SimpleIdGenerator;
import com.tencent.bk.base.datalab.queryengine.server.util.LauchArgs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class IdGeneratorLoader {

    @Autowired
    ApplicationArguments applicationArguments;

    @Bean
    SimpleIdGenerator getIdGenerator() {
        try {
            LauchArgs lauchArgs = new LauchArgs();
            JCommander commander = new JCommander(lauchArgs);
            commander.parse(applicationArguments.getSourceArgs());
            if (lauchArgs.help) {
                commander.usage();
            }
            log.info("IdGenerator init workId:{} dataCenterId:{}", lauchArgs.workId,
                    lauchArgs.datacenterId);
            SimpleIdGenerator idGenerator = new SimpleIdGenerator(lauchArgs.workId,
                    lauchArgs.datacenterId);
            return idGenerator;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create IdGenerator, pls check the sys.log!");
        }
    }

}
