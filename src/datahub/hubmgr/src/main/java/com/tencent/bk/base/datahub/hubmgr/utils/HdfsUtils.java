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

import static com.tencent.bk.base.datahub.databus.commons.Consts.PARAM_ERROR_RETCODE;
import static com.tencent.bk.base.datahub.databus.commons.utils.LogUtils.ERR_PREFIX;
import static com.tencent.bk.base.datahub.databus.commons.utils.LogUtils.HDFS_ERR;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_ALL_HDFS_CONFIG_PATH;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_ALL_HDFS_CONFIG_PATH_DEFAULT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_DATAHUB_DNS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DEFAULT_GEOG_AREA_TAG;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.GEOG_AREA;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.GEOG_AREAS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.HDFS_CONF;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsUtils {

    private static final Logger log = LoggerFactory.getLogger(HdfsUtils.class);

    /**
     * 删除hdfs文件
     *
     * @param conf 配置字符串
     * @param list 删除的日期列表
     * @return 删除是否成功
     */
    public static boolean remove(String conf, Set<String> list) {
        Map<String, Object> confMap = JsonUtils.toMapWithoutException(conf);
        if (confMap.isEmpty()) {
            LogUtils.warn(log, "confMap is empty");
            return false;
        }

        Map<String, Object> customProps = JsonUtils.toMapWithoutException((String) confMap.get("hdfsCustomProperty"));
        Configuration hdfsConf = new Configuration();
        for (Map.Entry<String, Object> entry : customProps.entrySet()) {
            hdfsConf.set(entry.getKey(), entry.getValue().toString());
        }

        List<Path> pathList = new ArrayList<>();
        for (String item : list) {
            String filePath = confMap.get("hdfsUrl") + "/" + confMap.get("topicsDir") + "/" + confMap.get("bizId") + "/"
                    + confMap.get("tableName") + item;
            pathList.add(new Path(filePath));
        }
        return remove((String) confMap.get("hdfsUrl"), hdfsConf, pathList);
    }

    /**
     * 删除hdfs文件
     *
     * @param url hdfs url
     * @param conf filesystem配置
     * @param filePaths 需要删除目录
     * @return 是否删除成功
     */
    public static boolean remove(String url, Configuration conf, List<Path> filePaths) {
        try {
            FileSystem fs = FileSystem.newInstance(URI.create(url), conf);
            for (Path p : filePaths) {
                if (fs.exists(p)) {
                    LogUtils.info(log, "begin to delete data: {}", p.toString());
                    fs.delete(p, true);
                }
            }
        } catch (IOException e) {
            LogUtils.warn(log, "failed to delete file", e);
            return false;
        }
        return true;
    }

    /**
     * 获取iceberg相关的所有hdfs集群的连接配置信息，包含hive metastore的地址信息
     *
     * @return iceberg相关的连接所有hdfs集群的配置信息
     */
    public static Map<String, String> getIcebergAllHdfsConf() {
        DatabusProps props = DatabusProps.getInstance();
        String geogArea = props.getOrDefault(GEOG_AREA, DEFAULT_GEOG_AREA_TAG);
        String apiDns = CommUtils.getDns(API_DATAHUB_DNS);
        String apiPath = props.getOrDefault(API_ALL_HDFS_CONFIG_PATH, API_ALL_HDFS_CONFIG_PATH_DEFAULT);
        String url = "http://" + apiDns + apiPath;
        Map<String, String> result = new HashMap<>();

        try {
            Map<String, Object> data = (Map<String, Object>) CommUtils.parseGetApiResult(url);
            List<String> geogAreas = (List<String>) data.get(GEOG_AREAS);
            if (geogAreas.contains(geogArea)) {
                String hiveUris = ((Map<String, String>) data.get(METASTOREURIS.varname)).get(geogArea);
                result.put(METASTOREURIS.varname, hiveUris);
                Map<String, String> hdfsConf = ((Map<String, String>) data.get(HDFS_CONF));
                result.putAll(hdfsConf);
            } else {
                LogUtils.warn(log, "{} geog area not exists in response {}", geogArea, data);
            }

        } catch (Exception e) {
            LogUtils.error(PARAM_ERROR_RETCODE, log, "failed to get iceberg all HDFS clusters config!", e);
        }

        return result;
    }

    /**
     * 初始化hadoop file system对象
     *
     * @param customConf hdfs连接配置信息
     * @return hadoop file system对象
     */
    public static FileSystem initHdfs(Map<String, String> customConf) {
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("fs.automatic.close", false);
            // 使用hdfs任务的custom properties来设置链接配置项
            customConf.forEach(conf::set);

            return FileSystem.newInstance(URI.create(customConf.get("fs.defaultFS")), conf);
        } catch (IOException e) {
            LogUtils.error(ERR_PREFIX + HDFS_ERR, log, " init hadoop filesystem failed!", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取path下的数据文件列表，按照文件名称先后顺序排列
     *
     * @param fs hadoop的file system对象
     * @param path 数据文件的目录
     * @param recursive 是否递归读取目录中的文件
     * @throws IOException 异常
     */
    public static List<FileStatus> getDataFilesInPath(FileSystem fs, Path path, boolean recursive) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(path);
        List<FileStatus> dataFiles = new ArrayList<>();

        if (fileStatuses != null) {
            for (FileStatus data : fileStatuses) {
                if (data.isFile()) {
                    dataFiles.add(data);
                } else if (data.isDirectory() && recursive) {
                    // 递归读取目录中的文件
                    dataFiles.addAll(getDataFilesInPath(fs, data.getPath(), true));
                }
            }
        }

        return dataFiles;
    }

    /**
     * 递归遍历HDFS上指定目录下的文件，获取符合条件的文件
     *
     * @param fs FileSystem对象
     * @param dir 目录名称
     * @param predicate 过滤条件
     * @param matchingFiles 符合条件的文件集合
     */
    public static void listDirRecursively(FileSystem fs, String dir, Predicate<FileStatus> predicate,
            List<String> matchingFiles) throws IOException {
        Path path = new Path(dir);
        List<String> subDirs = new ArrayList<>();

        for (FileStatus file : fs.listStatus(path, HiddenPathFilter.get())) {
            if (file.isDirectory()) {
                subDirs.add(file.getPath().toString());
            } else if (file.isFile() && predicate.test(file)) {
                matchingFiles.add(file.getPath().toString());
            }
        }

        for (String subDir : subDirs) {
            listDirRecursively(fs, subDir, predicate, matchingFiles);
        }
    }
}
