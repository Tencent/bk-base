
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

package com.tencent.bk.base.datahub.databus.connect.common.source.sample;

import com.tencent.bk.base.datahub.databus.connect.common.source.BkSourceTask;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleSourceTask extends BkSourceTask {

    private static final Logger log = LoggerFactory.getLogger(SimpleSourceTask.class);

    private SimpleSourceConfig config;
    private Map<String, String> partition;
    private Map<String, Object> offset;

    /**
     * 启动task,初始化资源和属性
     */
    @Override
    public void startTask() {
        config = new SimpleSourceConfig(configProperties);
        // 从topic中获取offset信息
        partition = Collections.singletonMap("SimpleSourceTask", config.dataFile);
        offset = context.offsetStorageReader().offset(partition);
        if (offset == null) {
            offset = new HashMap<>();
        } else {
            LogUtils.debug(log, "offset: {}", offset);
        }
    }

    /**
     * 从文件中读取数据，将数据放入结果集中
     *
     * @return 拉取到的数据集
     */
    @Override
    public List<SourceRecord> pollData() {
        final List<SourceRecord> results = new ArrayList<>();
        try (
                FileInputStream fis = new FileInputStream(config.dataFile);
                InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
                BufferedReader reader = new BufferedReader(isr)
        ) {
            // 从文件中读取数据,转化为list,写入目标文件中
            String line;
            int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                LogUtils.info(log, "line {} result {}", lineNum, line);
            }

            offset.put(config.dataFile, "" + lineNum);
            results.add(new SourceRecord(partition, offset, config.topic, null, "finish"));
        } catch (IOException e) {
            LogUtils.warn(log, "poll data exception!", e);
        }

        return results;
    }

}
