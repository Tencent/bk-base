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

package com.tencent.bk.base.datalab.queryengine.server.util;

import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.CSV;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.GZIP;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.ZIP;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.LIST;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.SELECT_FIELDS_ORDER;
import static com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants.DEVICE_TYPE_HDFS;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.SYSTEM_INNER_ERROR;
import static com.tencent.bk.base.datalab.queryengine.server.util.DownLoadUtil.getDownLoadFileName;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;

import com.tencent.bk.base.datahub.iceberg.BkTable;
import com.tencent.bk.base.datalab.meta.Field;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.tencent.bk.base.datalab.queryengine.common.character.BaseStringUtil;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.third.MetaApiService;
import com.tencent.bk.base.datalab.queryengine.server.third.StoreKitApiService;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@Slf4j
public class DataLakeUtil {

    public static final String STORAGES = "storages";
    public static final String DATA_TYPE = "data_type";
    public static final String ICEBERG = "iceberg";
    public static final String DFS_NAMESERVICES = "dfs.nameservices";
    public static final String DFS_HA_NAMENODES = "dfs.ha.namenodes";
    public static final String DFS_NAMENODE_RPC_ADDRESS = "dfs.namenode.rpc-address";
    public static final String CSV_DELIMITER = ",";
    public static final ThreadLocal<Boolean> WRITE_FIELDS_FIRST = new ThreadLocal<>();

    /**
     * 获取 BkTable 实例
     *
     * @param resultTableId 结果表 Id
     * @return BkTable 实例
     */
    public static BkTable getBkTable(String resultTableId) {
        StoragesProperty hdfsStorageProps = SpringBeanUtil.getBean(MetaApiService.class)
                .fetchTableStorages(resultTableId)
                .getStorages()
                .getAdditionalProperties()
                .get(DEVICE_TYPE_HDFS);
        String physicalTbName = hdfsStorageProps.getPhysicalTableName();
        String[] names = physicalTbName.split("\\.");
        String dbName = names[0];
        String tbName = names[1];
        Map<String, String> connMap =
                SpringBeanUtil.getBean(StoreKitApiService.class).fetchHdfsConf(resultTableId)
                        .entrySet()
                        .stream().collect(
                        Collectors.toMap(entry -> entry.getKey(),
                                entry -> entry.getValue().toString()));
        return new BkTable(dbName, tbName, connMap);
    }

    /**
     * 读取 hdfs iceberg 格式文件，生成查询结果集
     *
     * @param resultTableId 结果表 Id
     * @param size 行数
     * @return 查询结果集
     */
    public static Map<String, Object> readFromDataLake(String resultTableId, int size) {
        Map<String, Object> result = new HashMap<>(16);
        if (BaseStringUtil.isNotEmpty(resultTableId)) {
            BkTable table = getBkTable(resultTableId);
            table.loadTable();

            // iceberg存储的字段列表和selectFieldsOrder做映射
            Map<String, String> columnNameMapping = new LinkedHashMap<>(16);
            List<Record> recordList = table.sampleRecords(size);
            List<Map<String, Object>> records = new ArrayList<>();
            for (Record record : recordList) {
                if (columnNameMapping.isEmpty()) {
                    generateColumnMapping(record, resultTableId, columnNameMapping);
                }
                records.add(convertRecordToMap(record, columnNameMapping));
            }
            result.put(LIST, records);
            result.put(SELECT_FIELDS_ORDER, columnNameMapping.values());
        } else {
            result.put(LIST, new ArrayList<>());
            result.put(SELECT_FIELDS_ORDER, new ArrayList<>());
        }
        return result;
    }

    /**
     * 读取 hdfs iceberg 格式文件，并按照给定格式生成下载文件
     *
     * @param resultTableId 结果表 Id
     * @param downloadFormat 数据下载格式
     * @return 输出数据流
     */
    public static StreamingResponseBody convertRecordsToGivenType(String resultTableId,
            String downloadFormat) {
        BkTable table = getBkTable(resultTableId);
        table.loadTable();
        StreamingResponseBody responseBody = outputStream -> {
            WRITE_FIELDS_FIRST.set(true);
            switch (downloadFormat) {
                case GZIP:
                    outputGzipFile(outputStream, resultTableId, table);
                    break;
                case ZIP:
                    outputZipFile(outputStream, resultTableId, table);
                    break;
                default:
                    outputDataToFile(outputStream, resultTableId, table);
            }
            outputStream.close();
        };
        return responseBody;
    }

    /**
     * 输出 gzip 格式文件
     *
     * @param outputStream 输出数据流
     * @param resultTableId 结果表 Id
     * @param table BkTable 实例
     */
    private static void outputGzipFile(OutputStream outputStream,
            String resultTableId, BkTable table) throws IOException {
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        outputDataToFile(gzipOutputStream, resultTableId, table);
        gzipOutputStream.close();
    }

    /**
     * 输出 zip 格式文件
     *
     * @param outputStream 输出数据流
     * @param resultTableId 结果表 Id
     * @param table BkTable 实例
     */
    private static void outputZipFile(OutputStream outputStream,
            String resultTableId, BkTable table) throws IOException {
        ZipOutputStream zipOutputStream = new ZipOutputStream(outputStream);
        zipOutputStream.putNextEntry(new ZipEntry(getDownLoadFileName(CSV)));
        outputDataToFile(zipOutputStream, resultTableId, table);
        zipOutputStream.closeEntry();
        zipOutputStream.close();
    }

    /**
     * 往输出文件写数据
     *
     * @param outputStream 输出数据流
     * @param resultTableId 结果表 Id
     * @param table BkTable 实例
     */
    private static void outputDataToFile(OutputStream outputStream,
            String resultTableId, BkTable table) {
        // 读取所有数据
        Expression all = alwaysTrue();
        Map<String, String> columnNameMapping = new LinkedHashMap<>(16);
        try (CloseableIterable<Record> iter = table.readRecords(all)) {
            Iterator<Record> its = iter.iterator();
            while (its.hasNext()) {
                Record record = its.next();
                if (columnNameMapping.isEmpty()) {
                    generateColumnMapping(record, resultTableId, columnNameMapping);
                }
                writeRecord(outputStream, record, columnNameMapping);
            }
        } catch (IOException ignore) {
            throw new QueryDetailException(SYSTEM_INNER_ERROR);
        }
    }

    /**
     * 输出 record
     *
     * @param outputStream OutputStream
     * @param record Record
     * @param columnNameMapping Map
     */
    private static void writeRecord(OutputStream outputStream, Record record,
            Map<String, String> columnNameMapping) throws IOException {
        if (WRITE_FIELDS_FIRST.get() != null) {
            writeColumnNameFirst(outputStream, columnNameMapping);
        }
        boolean writeDelimiter = false;
        List<String> recordFields = new ArrayList<>();
        for (Types.NestedField recordField : record.struct().fields()) {
            recordFields.add(recordField.name());
        }
        for (String fieldName : columnNameMapping.keySet()) {
            if (writeDelimiter) {
                outputStream.write(CSV_DELIMITER.getBytes(StandardCharsets.UTF_8));
            }
            if (recordFields.contains(fieldName)) {
                String value = record.getField(fieldName) == null ? "" :
                        record.getField(fieldName).toString();
                // 处理value中含有特殊字符，csv文件按逗号分割错位问题
                if (value.contains(CSV_DELIMITER) || value.contains("\"") || value
                        .contains("'")) {
                    value = value.replace("\"", "\"\"");
                    value = String.format("\"%s\"", value);
                    outputStream.write(value.getBytes(StandardCharsets.UTF_8));
                } else if (writeBomFirst(value)) {
                    outputStream.write(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF});
                    outputStream.write(value.getBytes(StandardCharsets.UTF_8));
                } else {
                    outputStream.write(value.getBytes(StandardCharsets.UTF_8));
                }
            }
            writeDelimiter = true;
        }
        outputStream.write('\n');
    }

    /**
     * 首先记录表头信息
     *
     * @param outputStream OutputStream
     * @param columnNameMapping 表头字段
     */
    private static void writeColumnNameFirst(OutputStream outputStream,
            Map<String, String> columnNameMapping) throws IOException {
        boolean writeDelimiter = false;
        for (String fieldName : columnNameMapping.values()) {
            if (writeDelimiter) {
                outputStream.write(CSV_DELIMITER.getBytes(StandardCharsets.UTF_8));
            }
            // 解决导出csv文件乱码问题
            outputStream.write(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF});
            outputStream.write(fieldName.getBytes(StandardCharsets.UTF_8));
            writeDelimiter = true;
        }
        outputStream.write('\n');
        WRITE_FIELDS_FIRST.remove();
    }

    /**
     * 判断写入此值时是否需要先加 BOM 头
     *
     * @param value 原始字符串
     */
    private static boolean writeBomFirst(String value) {
        try {
            // dtEventTime型的数据在csv文件中显示成########的乱码问题，需先加BOM头
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            format.setLenient(false);
            format.parse(value);
            return true;
        } catch (Exception e) {
            // 其它情况不加BOM头，尤其是整型，否则不能在csv文件中做二次计算，例如求和等
            return false;
        }
    }

    /**
     * 将 record 格式数据转为 map 格式数据
     *
     * @param record 单条原始记录
     * @param columnNameMapping 字段映射集合
     * @return 转换格式后的记录
     */
    private static Map<String, Object> convertRecordToMap(Record record,
            Map<String, String> columnNameMapping) {
        Map<String, Object> values = new HashMap<>(16);
        List<Types.NestedField> fields = record.struct().fields();
        for (Types.NestedField field : fields) {
            String fieldName = field.name();
            Object fieldValue = record.getField(fieldName);
            values.put(columnNameMapping.get(fieldName), fieldValue);
        }
        return values;
    }

    /***
     * 生成 iceberg 字段和原始 select 字段的映射集
     *
     * @param record 结果记录
     * @param columnNameMapping 映射集
     */
    private static void generateColumnMapping(Record record, String resultTableId,
            Map<String, String> columnNameMapping) {
        List<String> fieldNames = new ArrayList<>();
        record.struct().fields()
                .forEach(field -> fieldNames.add(field.name()));

        MetaApiService metaApiService = SpringBeanUtil.getBean(MetaApiService.class);
        List<Field> fields = metaApiService.fetchTableFields(resultTableId).getFields();
        fields.sort(Comparator.comparing(Field::getFieldIndex));
        for (Field field : fields) {
            String columnName = field.getFieldName();
            if (fieldNames.contains(columnName.toLowerCase())) {
                columnNameMapping.put(columnName.toLowerCase(), field.getFieldAlias());
            }
        }
    }
}