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

package com.tencent.bk.base.datahub.databus.connect.source.hdfs.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsJsonReader implements RecordsReader {

    private static final Logger log = LoggerFactory.getLogger(HdfsJsonReader.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Path path;
    private String[] columns;
    private int processedCnt = 0;
    private BufferedReader br;
    private boolean reachEnd = false;

    /**
     * 构造函数
     *
     * @param fs hdfs的file system对象
     * @param path 文件的path对象
     * @param columns 需要读取的字段集合，顺序排列的
     * @throws IOException 异常
     */
    public HdfsJsonReader(FileSystem fs, Path path, String[] columns) throws IOException {
        this.path = path;
        this.columns = columns;
        br = new BufferedReader(new InputStreamReader(fs.open(path), StandardCharsets.UTF_8));
    }

    /**
     * 跳过指定数量的记录数
     *
     * @param skipCount 跳过的记录数
     * @throws IOException 异常
     */
    public void seekTo(int skipCount) throws IOException {
        if (skipCount > 0) {
            while (br.readLine() != null) {
                processedCnt++;
                if (processedCnt >= skipCount) {
                    break;
                }
            }
        }
        LogUtils.debug(log, "seeking to line {} of file {}, skipCount is set to {}", processedCnt, path.getName(),
                skipCount);
    }

    /**
     * 读取一定数量的记录
     *
     * @param maxCount 最多读取的记录数量
     * @return 记录列表
     * @throws IOException 异常
     */
    public List<List<Object>> readRecords(int maxCount) throws IOException {
        List<List<Object>> result = new ArrayList<>(maxCount);

        String line = br.readLine();
        while (line != null) {
            result.add(parseLine(line));
            processedCnt++;
            if (--maxCount == 0) {
                break;
            }
            line = br.readLine();
        }

        if (line == null) {
            reachEnd = true;
        }

        LogUtils.debug(log, "reading {} records in file {}", result.size(), path.getName());

        return result;
    }

    /**
     * 获取文件名
     *
     * @return 文件名称
     */
    public String getFilename() {
        return path.getName();
    }

    /**
     * 是否文件已读取完毕
     *
     * @return 文件是否到达EOF
     */
    public boolean reachEnd() {
        return reachEnd;
    }

    /**
     * 处理的文件的行数
     *
     * @return 处理过的文件的行数
     */
    public int processedCnt() {
        return processedCnt;
    }

    /**
     * 关闭资源
     */
    public void closeResource() {
        if (br != null) {
            try {
                br.close();
            } catch (Exception ignore) {
                LogUtils.warn(log, "{} failed to close buffer reader. {}", path.getName(), ignore.getMessage());
            }
        }
    }


    /**
     * 将行内的数据解析，转化为对象列表
     *
     * @param line 一行里的文本内容
     * @return 对象列表
     */
    private List<Object> parseLine(String line) {
        List<Object> result = new ArrayList<>(columns.length);
        try {
            Map<String, Object> record = OBJECT_MAPPER.readValue(line, HashMap.class);
            for (String column : columns) {
                result.add(record.getOrDefault(column, null));
            }
        } catch (IOException ioe) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.JSON_FORMAT_ERR,
                    String.format("file %s contains bad json: %s", path.getName(), line), ioe);
        }

        return result;
    }
}
