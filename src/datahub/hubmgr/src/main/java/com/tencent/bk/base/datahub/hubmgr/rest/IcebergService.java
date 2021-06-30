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

package com.tencent.bk.base.datahub.hubmgr.rest;

import com.google.common.base.Preconditions;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.IcebergParam;
import com.tencent.bk.base.datahub.iceberg.BkTable;
import com.tencent.bk.base.datahub.iceberg.C;
import com.tencent.bk.base.datahub.iceberg.CommitMsg;
import com.tencent.bk.base.datahub.iceberg.OpsMetric;
import com.tencent.bk.base.datahub.iceberg.PartitionMethod;
import com.tencent.bk.base.datahub.iceberg.TableField;
import com.tencent.bk.base.datahub.iceberg.TableUtils;
import com.tencent.bk.base.datahub.iceberg.Utils;
import com.tencent.bk.base.datahub.iceberg.functions.Assign;
import com.tencent.bk.base.datahub.iceberg.functions.Replace;
import com.tencent.bk.base.datahub.iceberg.functions.Substring;
import com.tencent.bk.base.datahub.iceberg.functions.Sum;
import com.tencent.bk.base.datahub.iceberg.functions.ValFunction;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/iceberg")
public class IcebergService {

    private static final Logger log = LoggerFactory.getLogger(IcebergService.class);
    private static final String OPERATION = "operation";
    private static final String RIGHT = "right";
    private static final String LEFT = "left";
    private static final String FIELD_NAME = "fieldName";
    private static final String PARTITIONS = "partitions";
    private static final String SEPARATOR = "separator";
    private static final String FUNCTION = "function";
    private static final String PARAMETERS = "parameters";
    private static final String METHOD = "method";
    private static final String TYPE = "type";
    private static final String VALUE = "value";
    private static final String SUM = "sum";
    private static final String ASSIGN = "assign";
    private static final String REPLACE = "replace";
    private static final String SUBSTRING = "substring";
    private static final String FIELD = "field";
    private static final String OK = "ok";
    private static final String FAILED = "failed";
    private static final String INTEGER = "integer";
    private static final String PARENT_ID = "parentId";
    private static final String SEQUENCE_NUMBER = "sequenceNumber";
    private static final String MANIFEST_LIST_LOCATION = "manifestListLocation";
    private static final String NAME = "name";
    private static final String NULLABLE = "nullable";
    private static final String TRUE = "true";
    private static final String ICEBERG = "iceberg";
    private static final String SOURCEID = "sourceId";
    private static final String FIELDID = "fieldId";
    private static final String TRANSFORM = "transform";
    private static final String DELETED = "deleted";
    private static final String MSG = "msg";

    // 这里最小的时间间隔单位设置为天，无论表按照小时还是天分区，均按照天过滤小数据文件然后合并
    private static final Map<String, ChronoUnit> PLUS_STEP_MAP = ImmutableMap.of("HOUR", ChronoUnit.DAYS,
            "DAY", ChronoUnit.DAYS, "MONTH", ChronoUnit.MONTHS, "YEAR", ChronoUnit.YEARS);
    private static final ExecutorService ES = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("iceberg-%d").build());

    /**
     * 建表
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/createTable")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult createTable(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            List<TableField> fields = params.getSchema()
                    .stream()
                    .map(e -> new TableField(e.get(NAME), e.get(TYPE),
                            Boolean.parseBoolean(e.getOrDefault(NULLABLE, TRUE))))
                    .collect(Collectors.toList());

            if (params.getPartition().size() == 0) {
                // 创建非分区的iceberg表
                table.createTable(fields);
            } else {
                List<PartitionMethod> partitionMethods = new ArrayList<>();
                for (Map<String, String> p : params.getPartition()) {
                    partitionMethods.add(new PartitionMethod(p.get(FIELD), p.get(METHOD)));
                }
                table.createTable(fields, partitionMethods);
            }

            return new ApiResult(Consts.NORMAL_RETCODE, OK, true, table.info(), null);
        } catch (Exception e) {
            LogUtils.error(Consts.SERVER_ERROR_RETCODE, log, table.toString() + " table already exists!", e);
            return new ApiResult(Consts.SERVER_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 变更表结构
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/alterTable")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult alterTable(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            Map<String, String> toRename = params.getRenameFields();
            // rename操作与remove或者add操作不允许同时存在，否则忽略后者
            if (toRename.isEmpty()) {
                List<TableField> toAdd = params.getAddFields()
                        .stream()
                        .map(e -> new TableField(e.get(NAME), e.get(TYPE),
                                Boolean.parseBoolean(e.getOrDefault(NULLABLE, TRUE))))
                        .collect(Collectors.toList());
                Set<String> toRemove = new HashSet<>(params.getRemoveFields());
                if (toAdd.isEmpty() && toRemove.isEmpty()) {
                    return new ApiResult(Consts.PARAM_ERROR_RETCODE, "nothing need to alter !", false);
                } else if (toAdd.isEmpty()) {
                    table.removeFields(toRemove);
                } else if (toRemove.isEmpty()) {
                    table.addFields(toAdd);
                } else {
                    table.modifyFields(toAdd, toRemove);
                }
            } else {
                table.renameFields(toRename);
            }
            LogUtils.info(log, "{} table schema changed. {}", table.toString(), table.info());

            return new ApiResult(Consts.NORMAL_RETCODE, OK, true, table.info(), null);
        } catch (Exception e) {
            LogUtils.error(Consts.SERVER_ERROR_RETCODE, log, table.toString() + " alter table failed!", e);
            return new ApiResult(Consts.SERVER_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 修改表的分区定义
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/changePartitionSpec")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult changePartitionSpec(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            List<PartitionMethod> partitionMethods = new ArrayList<>();
            for (Map<String, String> p : params.getPartition()) {
                partitionMethods.add(new PartitionMethod(p.get(FIELD), p.get(METHOD)));
            }
            table.changePartitionSpec(partitionMethods);

            return new ApiResult(Consts.NORMAL_RETCODE, OK, true, table.info(), null);
        } catch (Exception e) {
            LogUtils.error(Consts.SERVER_ERROR_RETCODE, log, table.toString() + " change table partition spec failed!",
                    e);
            return new ApiResult(Consts.SERVER_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 修改表的元信息文件存储位置
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/updateLocation")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult updateLocation(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            table.updateLocation(params.getLocation());

            return new ApiResult(Consts.NORMAL_RETCODE, OK, true);
        } catch (Exception e) {
            LogUtils.error(Consts.SERVER_ERROR_RETCODE, log, table.toString() + " update table location failed!", e);
            return new ApiResult(Consts.SERVER_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 获取表location信息
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/location")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult location(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            return new ApiResult(table.currentLocation());
        } catch (Exception e) {
            LogUtils.error(Consts.SERVER_ERROR_RETCODE, log, table.toString() + " query table location failed!", e);
            return new ApiResult(Consts.SERVER_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 删除指定表分区数据
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/deletePartitions")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult deletePartitions(final IcebergParam params) {
        Map<String, String> delPartition = params.getDeletePartition();
        if (delPartition.containsKey(FIELD_NAME) && delPartition.containsKey(PARTITIONS)
                && delPartition.containsKey(SEPARATOR)) {
            Set<String> partitionSet = Stream
                    .of(StringUtils.split(delPartition.get(PARTITIONS), delPartition.get(SEPARATOR)))
                    .collect(Collectors.toSet());
            BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());

            try {
                table.loadTable();
                OpsMetric metric = table.deletePartitions(delPartition.get(FIELD_NAME), partitionSet);
                return new ApiResult(metric.summary());
            } catch (Exception e) {
                LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " delete partitions failed!", e);
                return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
            }
        } else {
            String msg = "IllegalArgumentException：not found key fieldName, partitions or separator in parameters: "
                    + delPartition.toString();
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, msg);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, msg, false);
        }
    }

    /**
     * 按条件删除数据行
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/deleteData")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult deleteData(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        Map<Object, Object> condition = params.getConditionExpression();
        try {
            table.loadTable();
            OpsMetric metric = table.deleteData(parseCondition(table, condition));
            return new ApiResult(metric.summary());
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log,
                    table.toString() + " table deleteData failed with condition: " + condition.toString(), e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 删除表的元数据及数据
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/updateData")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult updateData(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            Expression expr = parseCondition(table, params.getConditionExpression());
            Map<String, ValFunction> transforms = new HashMap<>();
            params.getAssignExpression().forEach((k, v) -> {
                Optional<ValFunction> valFunction = getValFunction(v);
                Preconditions.checkArgument(valFunction.isPresent(),
                        "failed to updateData, invalid parameter: " + v.toString());
                transforms.put(k, valFunction.get());
            });
            OpsMetric metric = table.updateData(expr, transforms);
            return new ApiResult(metric.summary());
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " table updateData failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 删除表的元数据及数据
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/dropTable")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult dropTable(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());

        try {
            boolean result = table.dropTable();
            return new ApiResult(result);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " drop table failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 强制释放hive metastore中此表相关的锁
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/forceReleaseLock")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult forceReleaseLock(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());

        try {
            boolean result = table.forceReleaseLock();
            return new ApiResult(result);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " force release lock failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 重命名表
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/renameTable")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult renameTable(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        String newTableName = params.getNewTableName();
        if ("".equals(newTableName)) {
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, "invalid parameter: empty newTableName", false);
        }

        try {
            table.loadTable();  // 确保表存在
            table.renameTable(newTableName);
            return new ApiResult(Consts.NORMAL_RETCODE, OK, true);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " rename table failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 删除过期时间以外的数据
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/expire")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult expire(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            table.expireEarlyData(params.getTimeField(), params.getExpireDays());
            return new ApiResult(Consts.NORMAL_RETCODE, OK, true);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " expire data failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 合并小数据文件
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/compact")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult compact(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            final LocalDate start = LocalDate.parse(params.getCompactStart(), DateTimeFormatter.BASIC_ISO_DATE);
            final LocalDate end = LocalDate.parse(params.getCompactEnd(), DateTimeFormatter.BASIC_ISO_DATE);
            Preconditions.checkArgument(start != null && end != null,
                    String.format("invalid parameter: %s, %s", params.getCompactStart(), params.getCompactEnd()));

            OffsetTime zero = OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC);
            if (params.getAsyncCompact()) {
                // 获取表时间分区定义
                List<String> transforms = table.currentPartitionInfo().stream()
                        .filter(p -> p.name().startsWith(C.ET))
                        .map(p -> p.transform().toString())
                        .collect(Collectors.toList());
                // 取第一个，正常情况下也只有一个ET字段的分区函数
                if (transforms.size() == 0) {
                    return new ApiResult(Consts.PARAM_ERROR_RETCODE,
                            table.toString() + " has no timestamp partition, unable compact", false);
                }

                // 未来可以考虑将任务信息写入ZK指定路径上，便于查看异步任务相关信息，也避免某张表同时触发多个合并任务。
                String pMethod = transforms.get(0).toUpperCase();
                ES.submit(() -> {
                    // 按照指定的步长计算compact的起止时间
                    ChronoUnit step = PLUS_STEP_MAP.getOrDefault(pMethod, ChronoUnit.DAYS);
                    OffsetDateTime compactStart = start.atTime(zero);
                    OffsetDateTime nextStart = compactStart.plus(1, step);
                    OffsetDateTime compactEnd = end.atTime(zero);

                    try {
                        do {
                            OffsetDateTime currentEnd = compactEnd.isBefore(nextStart) ? compactEnd : nextStart;
                            log.info("{}: compacting files between [{}, {})", table, compactStart, currentEnd);
                            table.compactDataFile(C.ET, compactStart, currentEnd);

                            // 进入下一段合并小文件的周期
                            compactStart = nextStart;
                            nextStart = compactStart.plus(1, step);
                        } while (compactStart.isBefore(compactEnd));

                        log.info("{}: finish compacting files between [{}, {})", table, start, end);
                    } catch (Exception e) {
                        log.error("{} async compact table data between [{}, {}) failed: {}",
                                table, params.getCompactStart(), params.getCompactEnd(), e.getMessage());
                    }

                });
            } else {
                table.compactDataFile(params.getTimeField(), start.atTime(zero), end.atTime(zero));
            }

            return new ApiResult(Consts.NORMAL_RETCODE, OK, true);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " compact data file failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 返回表的基本信息
     *
     * @param params 参数列表
     * @return 返回查询结果集
     */
    @POST
    @Path("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult info(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            return new ApiResult(table.info());
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " get info failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 返回表的分区信息
     *
     * @param params 参数列表
     * @return 返回查询结果集
     */
    @POST
    @Path("/partitionInfo")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult partitionInfo(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            List<Map<String, Object>> result = new ArrayList<>();
            table.currentPartitionInfo().forEach(f -> {
                Map<String, Object> m = new HashMap<>();
                m.put(SOURCEID, f.sourceId());
                m.put(FIELDID, f.fieldId());
                m.put(NAME, f.name());
                m.put(TRANSFORM, f.transform().toString().toUpperCase());

                result.add(m);
            });

            return new ApiResult(result);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " get partition info failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 返回表的schema信息
     *
     * @param params 参数列表
     * @return 返回查询结果集
     */
    @POST
    @Path("/schemaInfo")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult schemaInfo(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            return new ApiResult(table.fieldsInfo());
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " get schema info failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 列出table所在catalog的所有库表信息
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/listCatalog")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult listCatalog(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            Map<String, List<String>> catalog = table.listCatalog()
                    .entrySet()
                    .stream()
                    .filter(e -> e.getKey().toString().contains(ICEBERG))
                    .collect(Collectors.toMap(e -> e.getKey().toString(),
                            e -> e.getValue().stream().map(TableIdentifier::toString).collect(Collectors.toList())));
            return new ApiResult(catalog);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " list catalog failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 返回表的快照信息
     *
     * @param params 参数列表
     * @return 返回查询结果集
     */
    @POST
    @Path("/snapshots")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult snapshots(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            return new ApiResult(getSnapshots(table, params.getRecordNum()));
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " get snapshots failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 返回表的commitMsg信息
     *
     * @param params 参数列表
     * @return 返回查询结果集
     */
    @POST
    @Path("/commitMsgs")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult commitMsgs(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            return new ApiResult(table.getLatestCommitMsg(params.getRecordNum()));
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " get commitMsgs failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 返回表的分区路径列表
     *
     * @param params 参数列表
     * @return 返回查询结果集
     */
    @POST
    @Path("/partitionPaths")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult partitionPaths(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            return new ApiResult(table.getPartitionPaths());
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " get partition paths failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 返回表的采样数据
     *
     * @param params 参数列表
     * @return 返回查询结果集
     */
    @POST
    @Path("/sampleRecords")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult sampleRecords(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            List<Map<String, Object>> data = table.getLatestRecords(params.getRecordNum())
                    .stream()
                    .map(r -> toMap(r, table.schema()))
                    .collect(Collectors.toList());

            return new ApiResult(data);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " get sample records failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 返回表的采样数据
     *
     * @param params 参数列表
     * @return 返回查询结果集
     */
    @POST
    @Path("/readRecords")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult readRecords(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            Expression expr = parseCondition(table, params.getConditionExpression());
            List<Map<String, Object>> data = getRecords(table, expr, params.getRecordNum())
                    .stream()
                    .map(r -> toMap(r, table.schema()))
                    .collect(Collectors.toList());

            return new ApiResult(data);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " read records failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 设置其offset
     *
     * @param params 参数列表
     * @return 返回查询结果集
     */
    @POST
    @Path("/resetOffset")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult resetOffset(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            Map<String, String> offsetMsgs = params.getOffsetMsgs();
            Preconditions.checkArgument(!offsetMsgs.isEmpty(), "parameter error: empty offsetMsgs");
            table.addCommitMsg(new CommitMsg(C.DT_RESET, offsetMsgs));
            return new ApiResult(table.getLatestCommitMsg(1));
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " reset offset failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 更新表属性
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/updateProperties")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult updateProperties(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            Map<String, String> properties = params.getUpdateProperties();
            if (properties.isEmpty()) {
                return new ApiResult(Consts.PARAM_ERROR_RETCODE, "empty properties!", false);
            }
            table.updateProperties(properties);
            return new ApiResult(Consts.NORMAL_RETCODE, OK, true);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " update properties failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 从iceberg表中删除指定的数据文件
     *
     * @param params 参数列表
     * @return 返回执行结果
     */
    @POST
    @Path("/deleteDatafiles")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult deleteDatafiles(final IcebergParam params) {
        BkTable table = new BkTable(params.getDatabaseName(), params.getTableName(), params.getConfig());
        try {
            table.loadTable();
            log.info("{}: to delete files {} with msg {}", table, params.getFiles(), params.getMsg());
            Set<DataFile> toDelete = table.getAllDataFiles()
                    .stream()
                    .filter(df -> params.getFiles().contains(df.path().toString()))
                    .collect(Collectors.toSet());

            if (toDelete.size() > 0) {
                String delFiles = toDelete.stream().map(f -> f.path().toString()).collect(Collectors.joining(","));
                TableUtils.deleteTableDataFiles(table, toDelete,
                        ImmutableMap.of(DELETED, delFiles, MSG, params.getMsg()));
                LogUtils.info(log, "{}: deleted data files are {}", table, delFiles);
            } else {
                LogUtils.warn(log, "{}: no matching data files found to delete, param is {}", table, params.getFiles());
            }

            return new ApiResult(Consts.NORMAL_RETCODE, OK, true);
        } catch (Exception e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, table.toString() + " delete datafiles failed!", e);
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
    }

    /**
     * 生成ValFunction
     *
     * @param operation 代表一个操作，由函数名及其参数列表组成
     * @return ValFunction的Optional对象
     */
    private Optional<ValFunction> getValFunction(Map<String, Object> operation) {
        //前置检查：function, parameters, type, value等参数是否存在
        if (!(operation.containsKey(FUNCTION) && operation.containsKey((PARAMETERS)))) {
            return Optional.empty();
        }
        String function = operation.get(FUNCTION).toString().toLowerCase();
        List<Map<String, Object>> parameters = (List<Map<String, Object>>) operation.get(PARAMETERS);
        if (!checkParameters(function, parameters)) {
            return Optional.empty();
        }

        switch (function) {
            case SUM:
                Number toAdd = (Number) typeConvert((String) parameters.get(0).get(TYPE), parameters.get(0).get(VALUE));
                return Optional.of(new Sum(toAdd));
            case ASSIGN:
                Object value = typeConvert((String) parameters.get(0).get(TYPE), parameters.get(0).get(VALUE));
                return Optional.of(new Assign(value));
            case REPLACE:
                String fieldToUse = (String) typeConvert((String) parameters.get(0).get(TYPE),
                        parameters.get(0).get(VALUE));
                String toReplace = (String) typeConvert((String) parameters.get(1).get(TYPE),
                        parameters.get(1).get(VALUE));
                String replacement = (String) typeConvert((String) parameters.get(2).get(TYPE),
                        parameters.get(2).get(VALUE));
                return Optional.of(new Replace(fieldToUse, toReplace, replacement));
            case SUBSTRING:
                fieldToUse = (String) typeConvert((String) parameters.get(0).get(TYPE), parameters.get(0).get(VALUE));
                int start = (int) typeConvert((String) parameters.get(1).get(TYPE), parameters.get(1).get(VALUE));
                int end = (int) typeConvert((String) parameters.get(2).get(TYPE), parameters.get(2).get(VALUE));
                return Optional.of(new Substring(fieldToUse, start, end));
            default:
                return Optional.empty();
        }
    }

    /**
     * 校验updateData操作的参数列表与类型的一致性
     *
     * @param function updateData操作的类型
     * @param params updateData操作的参数列表
     * @return 校验结果
     */
    private boolean checkParameters(String function, List<Map<String, Object>> params) {
        // 参数列表不能为空
        if (params.isEmpty()) {
            return false;
        }

        switch (function) {
            case ASSIGN:
            case SUM:
                // ASSIGN和SUM类型操作的参数列表必须为1
                if (params.size() != 1) {
                    return false;
                }
                break;
            case REPLACE:
            case SUBSTRING:
                // REPLACE和SUBSTRING类型操作的参数列表必须为3
                if (params.size() != 3) {
                    return false;
                }
                break;
            default:
                return false;
        }

        // 检查参数类表是否是TYPE和VALUE组成的元组
        for (Map<String, Object> param : params) {
            if (!(param.containsKey(TYPE) && param.containsKey(VALUE))) {
                return false;
            }
        }

        return true;
    }

    /**
     * 解析存放在map中树形结构的表达式，转换为iceberg中的表达式
     *
     * @param table BkTable结构体
     * @param expr 包含表达式结构的map
     * @return iceberg的Expression对象
     */
    private Expression parseCondition(BkTable table, Map<Object, Object> expr) {
        Map<String, String> schema = new HashMap<>();
        table.fieldsInfo().forEach(f -> schema.put(f.get(C.NAME).toString(), f.get(C.TYPE).toString()));
        parameterConvert(schema, expr);

        return Utils.parseExpressions(expr);
    }

    /**
     * 参数检测与转换
     *
     * @param schema 表结构信息，fieldName与type映射
     * @param expr 包含表达式结构的map
     */
    private void parameterConvert(Map<String, String> schema, Map<Object, Object> expr) {
        String op = expr.get(OPERATION).toString().toUpperCase();
        Object left = expr.get(LEFT);
        Object right = expr.get(RIGHT);
        // 不支持NOT/True/False/STARTS_WITH操作符
        switch (Expression.Operation.valueOf(op)) {
            case GT:
            case LT:
            case EQ:
            case GT_EQ:
            case LT_EQ:
            case NOT_EQ:
                expr.replace(RIGHT, typeConvert(schema.get(left.toString()), right));
                break;
            case IN:
            case NOT_IN:
                Preconditions.checkArgument(right instanceof List && !((List) right).isEmpty(),
                        "right must be List and can not be empty");
                List values = new ArrayList<>();
                for (Object e : (List) right) {
                    values.add(typeConvert(schema.get(left.toString()), e));
                }
                expr.replace(RIGHT, values);
                break;
            case AND:
            case OR:
                parameterConvert(schema, (Map) left);
                parameterConvert(schema, (Map) right);
                break;
            case IS_NULL:
            case NOT_NULL:
                break;
            default:
                throw new IllegalArgumentException("unsupported expressions: " + expr);
        }
    }

    /**
     * 类型转换
     *
     * @param type 类型
     * @param value 待转换数据，来自java自动解析参数的结果
     * @return 转换结果
     */
    private Object typeConvert(String type, Object value) {
        switch (type.toLowerCase()) {
            case INTEGER:
            case C.INT:
                Preconditions.checkArgument(value instanceof Integer,
                        "bad type for int: " + value.getClass().getTypeName());
                return value;
            case C.LONG:
                Preconditions.checkArgument(value instanceof Integer || value instanceof Long,
                        "bad type for long: " + value.getClass().getTypeName());
                return ((Number) value).longValue();
            case C.FLOAT:
                Preconditions.checkArgument(value instanceof Float || value instanceof Integer
                                || value instanceof Long || value instanceof Double,
                        "bad type for float: " + value.getClass().getTypeName());
                return ((Number) value).floatValue();
            case C.DOUBLE:
                Preconditions.checkArgument(value instanceof Float || value instanceof Double
                                || value instanceof Integer || value instanceof Long,
                        "bad type for double: " + value.getClass().getTypeName());
                return ((Number) value).doubleValue();
            case C.STRING:
            case C.TEXT:
                Preconditions.checkArgument(value instanceof String,
                        "bad type for text: " + value.getClass().getTypeName());
                return value;
            case C.TIMESTAMP:
                Preconditions.checkArgument(value instanceof String && isValidTime((String) value),
                        "bad type for timestamp: " + value.getClass().getTypeName());
                return value;
            default:
                throw new IllegalArgumentException("unsupported table field type, something wrong with current table");
        }
    }

    /**
     * 校验时间字符串否合法
     *
     * @param time 时间字符串
     * @return 校验结果
     */
    private boolean isValidTime(String time) {
        try {
            OffsetDateTime.parse(time);
        } catch (DateTimeParseException e) {
            LogUtils.error(Consts.PARAM_ERROR_RETCODE, log, "time parse failed: " + time, e);
            return false;
        }

        return true;
    }

    /**
     * 获取当前快照记录
     *
     * @param table BkTable对象
     * @param recordNum 欲获取的记录数
     * @return 查询结果
     */
    private List<Map<String, Object>> getSnapshots(BkTable table, int recordNum) {
        return table.getLatestSnapshots(recordNum)
                .stream()
                .map(snapshot -> {
                    Map<String, Object> m = new HashMap<>();
                    m.put(MANIFEST_LIST_LOCATION, snapshot.manifestListLocation());
                    m.put(PARENT_ID, String.valueOf(snapshot.parentId()));
                    m.put(SEQUENCE_NUMBER, String.valueOf(snapshot.sequenceNumber()));
                    m.put(C.ID, String.valueOf(snapshot.snapshotId()));
                    m.put(C.TIMESTAMP_MS, String.valueOf(snapshot.timestampMillis()));
                    m.put(C.OPERATION, snapshot.operation());
                    m.put(C.SUMMARY, snapshot.summary());
                    return m;
                })
                .collect(Collectors.toList());
    }

    /**
     * 将Record中的value数据转成List输出
     *
     * @param r Record对象
     * @param schema 表的schema信息
     * @return value的链表对象
     */
    private Map<String, Object> toMap(Record r, Schema schema) {
        Map<String, Object> map = new LinkedHashMap<>();
        schema.columns().forEach(s -> {
            Object value = r.getField(s.name());
            // OffsetDateTime类型的结构体打印冗长，可读性差，使用toString()转换成字符串
            map.put(s.name(), value instanceof OffsetDateTime ? value.toString() : value);
        });

        return map;
    }

    /**
     * 按照条件查询数据
     *
     * @param table BkTable对象
     * @param condition 过滤条件的表达式
     * @param recordNum 欲获取的记录数
     * @return 查询结果
     */
    private List<Record> getRecords(BkTable table, Expression condition, int recordNum) {
        List<Record> records = new ArrayList<>(recordNum);
        try (CloseableIterable<Record> it = table.readRecords(condition)) {
            for (Record r : it) {
                records.add(r);
                if (records.size() == recordNum) {
                    break;
                }
            }
            return records;
        } catch (IOException ioe) {
            throw new UncheckedIOException("Failed to close CloseableIterable<Record>", ioe);
        }
    }

}
