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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.mysql;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MySqlJobTaskTypeDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.ExecuteStatusCache;
import com.tencent.bk.base.common.crypt.Crypt;
import com.tencent.bk.base.common.crypt.CryptException;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.node.NodeLabel;
import com.tencent.bk.base.dataflow.jobnavi.node.NodeLabelHost;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.RecoveryExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.db.ConnectionPool;
import com.tencent.bk.base.dataflow.jobnavi.util.db.SqlFileParser;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class MySqlDao extends AbstractJobDao {

    private static final Logger LOGGER = Logger.getLogger(MySqlDao.class);

    private static final String SQL_VERSION_TABLE_EXIST
            = "SELECT count(*) FROM INFORMATION_SCHEMA.TABLES "
            + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'jobnavi_version_config'";
    private static final String SQL_VERSION_CHECK = "SELECT max(version) AS version FROM jobnavi_version_config";

    private static final MySqlJobScheduleDao MY_SQL_JOB_SCHEDULE_DAO = new MySqlJobScheduleDao();
    private static final MySqlJobExecuteSqlDao MY_SQL_JOB_EXECUTE_SQL_DAO = new MySqlJobExecuteSqlDao();
    private static final MySqlJobEventDao MY_SQL_JOB_EVENT_DAO = new MySqlJobEventDao();
    private static final MySqlJobTaskTypeDao MY_SQL_JOB_TASK_TYPE_DAO = new MySqlJobTaskTypeDao();
    private static final MySqlJobNodeLabelDao MY_SQL_JOB_NODE_LABEL_DAO = new MySqlJobNodeLabelDao();
    private static final MySqlJobSavepointDao MY_SQL_JOB_SAVEPOINT_DAO = new MySqlJobSavepointDao();
    private static final MySqlJobExecuteStatusCacheDao MY_SQL_JOB_EXECUTE_STATUS_CACHE_DAO
            = new MySqlJobExecuteStatusCacheDao();

    @Override
    public void init(Configuration conf) throws NaviException {
        String url = conf.getString(Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_URL);
        if (url == null) {
            throw new NaviException("Mysql jdbc url cannot Empty! Please config "
                    + Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_URL + " and restart Scheduler");
        }

        String user = conf.getString(Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_USER);
        if (user == null) {
            user = "";
        }

        String password = conf.getString(Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_PASSWORD);
        if (password == null) {
            password = "";
        } else {
            boolean encrypt = conf.getBoolean(Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_PASSWORD_ENCRYPT,
                    Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_PASSWORD_ENCRYPT_DEFAULT);
            if (encrypt) {
                String instanceKey = conf.getString(Constants.JOBNAVI_CRYPT_INSTANCE_KEY);
                if (StringUtils.isEmpty(instanceKey)) {
                    throw new NaviException(Constants.JOBNAVI_CRYPT_ROOT_KEY + " can not be null if "
                            + Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_PASSWORD_ENCRYPT + " set true.");
                }
                String rootKey = conf.getString(Constants.JOBNAVI_CRYPT_ROOT_KEY);
                String rootIV = conf.getString(Constants.JOBNAVI_CRYPT_ROOT_IV);
                try {
                    password = new String(
                            Crypt.decrypt(password, rootKey, rootIV, instanceKey),
                            StandardCharsets.UTF_8);
                } catch (CryptException e) {
                    LOGGER.error("decrypt password error.", e);
                    throw new NaviException("decrypt password error. " + e.getMessage());
                }
            }
        }

        String mysqlDriverClass = "com.mysql.jdbc.Driver";
        try {
            ConnectionPool.initPool(url, user, password, mysqlDriverClass);
        } catch (SQLException | PropertyVetoException e) {
            LOGGER.error("Init DB Connnection Pool error");
            throw new NaviException(e);
        }
    }

    @Override
    public String getCurrentVersion() throws NaviException {
        String currentVersion = Constants.JOBNAVI_NULL_VERSION;
        if (!isVersionTableExist()) {
            return Constants.JOBNAVI_NULL_VERSION;
        }
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_VERSION_CHECK);
            rs = ps.executeQuery();
            while (rs.next()) {
                currentVersion = rs.getString("version");
                if (currentVersion == null) {
                    currentVersion = Constants.JOBNAVI_NULL_VERSION;
                }
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return currentVersion;
    }

    @Override
    public void upgrade(String version, String dbVersion) throws NaviException {
        if (Constants.JOBNAVI_NULL_VERSION.equals(dbVersion)) {
            upgradeFull(version);
        } else {
            upgradeDelta(version, dbVersion);
        }
    }

    //schedule view
    @Override
    public void addScheduleInfo(ScheduleInfo info) throws NaviException {
        MY_SQL_JOB_SCHEDULE_DAO.addScheduleInfo(info);
    }

    @Override
    public void deleteScheduleInfo(String scheduleId) throws NaviException {
        MY_SQL_JOB_SCHEDULE_DAO.deleteScheduleInfo(scheduleId);
    }

    @Override
    public void updateScheduleInfo(ScheduleInfo info) throws NaviException {
        MY_SQL_JOB_SCHEDULE_DAO.updateScheduleInfo(info);
    }

    @Override
    public void updateScheduleStartTime(String scheduleId, Long startTimeMills) throws NaviException {
        MY_SQL_JOB_SCHEDULE_DAO.updateScheduleStartTime(scheduleId, startTimeMills);
    }

    @Override
    public List<ScheduleInfo> listScheduleInfo() throws NaviException {
        return MY_SQL_JOB_SCHEDULE_DAO.listScheduleInfo();
    }

    @Override
    public Map<String, Long> listCurrentScheduleTime() throws NaviException {
        return MY_SQL_JOB_SCHEDULE_DAO.listCurrentScheduleTime();
    }

    @Override
    public void activeSchedule(String scheduleId, boolean active) throws NaviException {
        MY_SQL_JOB_SCHEDULE_DAO.activeSchedule(scheduleId, active);
    }

    @Override
    public List<String> listExpireSchedule(long expire) throws NaviException {
        return MY_SQL_JOB_SCHEDULE_DAO.listExpireSchedule(expire);
    }

    @Override
    public List<DependencyInfo> getParents(String scheduleId) throws NaviException {
        return MY_SQL_JOB_SCHEDULE_DAO.getParents(scheduleId);
    }

    @Override
    public List<String> getChildrenByParentId(String scheduleId) throws NaviException {
        return MY_SQL_JOB_SCHEDULE_DAO.getChildrenByParentId(scheduleId);
    }

    //execute view
    @Override
    public void addExecute(TaskEvent event) throws NaviException {
        MY_SQL_JOB_EXECUTE_SQL_DAO.addExecute(event);
    }

    @Override
    public long getCurrentExecuteIdIndex() throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.getCurrentExecuteIdIndex();
    }

    @Override
    public void updateExecuteHost(long executeId, String host) throws NaviException {
        MY_SQL_JOB_EXECUTE_SQL_DAO.updateExecuteHost(executeId, host);
    }

    @Override
    public void updateExecuteStatus(long executeId, TaskStatus status, String info) throws NaviException {
        MY_SQL_JOB_EXECUTE_SQL_DAO.updateExecuteStatus(executeId, status, info);
    }

    @Override
    public void updateExecuteRunningTime(long executeId) throws NaviException {
        MY_SQL_JOB_EXECUTE_SQL_DAO.updateExecuteRunningTime(executeId);
    }

    @Override
    public TaskStatus getExecuteStatus(long executeId) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.getExecuteStatus(executeId);
    }

    @Override
    public TaskStatus getExecuteStatus(String scheduleId, long scheduleTime) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.getExecuteStatus(scheduleId, scheduleTime);
    }

    @Override
    public ExecuteResult queryExecuteResult(long executeId) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteResult(executeId);
    }

    @Override
    public ExecuteResult queryLastExecuteResult(String scheduleId, Long scheduleTime) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryLastExecuteResult(scheduleId, scheduleTime);
    }

    @Override
    public ExecuteResult queryLatestExecuteResult(String scheduleId, Long scheduleTime) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryLatestExecuteResult(scheduleId, scheduleTime);
    }

    @Override
    public ExecuteResult queryLatestExecuteResultByDataTime(String scheduleId, Long dataTime) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryLatestExecuteResultByDataTime(scheduleId, dataTime);
    }

    @Override
    public List<ExecuteResult> queryExecuteResultByTimeRange(String scheduleId, Long[] timeRange) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteResultByTimeRange(scheduleId, timeRange);
    }

    @Override
    public List<ExecuteResult> queryExecuteResultByDataTimeRange(String scheduleId, Long[] timeRange)
            throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteResultByDataTimeRange(scheduleId, timeRange);
    }

    @Override
    public List<ExecuteResult> queryExecuteResultByStatus(String scheduleId, TaskStatus status) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteResultByStatus(scheduleId, status);
    }

    @Override
    public List<ExecuteResult> queryExecuteResultByTimeAndStatus(String scheduleId, Long scheduleTime,
            TaskStatus status) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteResultByTimeAndStatus(scheduleId, scheduleTime, status);
    }

    @Override
    public List<ExecuteResult> queryExecuteResultBySchedule(String scheduleId, Long limit) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteResultBySchedule(scheduleId, limit);
    }

    @Override
    public List<ExecuteResult> queryExecuteResultByHost(String host, TaskStatus status) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteResultByHost(host, status);
    }

    @Override
    public List<ExecuteResult> queryPreparingExecute(String scheduleId) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryPreparingExecute(scheduleId);
    }

    @Override
    public List<ExecuteResult> queryFailedExecutes(long beginTime, long endTime, String typeId) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryFailedExecutes(beginTime, endTime, typeId);
    }

    @Override
    public List<Map<String, Object>> queryExecuteByTimeRange(String scheduleId, Long startTime, Long endTime,
            Integer limit) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteByTimeRange(scheduleId, startTime, endTime, limit);
    }

    @Override
    public List<Map<String, Object>> queryExecuteByTimeRangeAndStatus(String scheduleId, Long startTime, Long endTime,
            TaskStatus status, Integer limit) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO
                .queryExecuteByTimeRangeAndStatus(scheduleId, startTime, endTime, status, limit);
    }

    @Override
    public List<Map<String, Object>> queryAllExecuteByTimeRange(String scheduleId, Long startTime, Long endTime,
            Integer limit) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryAllExecuteByTimeRange(scheduleId, startTime, endTime, limit);
    }

    @Override
    public Map<String, Map<String, Integer>> queryExecuteStatusAmount(Long startTime, Long endTime)
            throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteStatusAmount(startTime, endTime);
    }

    @Override
    public Map<String, Map<String, Integer>> queryExecuteStatusAmountByCreateAt(Long startTime, Long endTime)
            throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryExecuteStatusAmountByCreateAt(startTime, endTime);
    }

    @Override
    public Map<String, Integer> queryRunningExecuteNumbers() throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryRunningExecuteNumbers();
    }

    @Override
    public void deleteExpireExecute(long expire) throws NaviException {
        MY_SQL_JOB_EXECUTE_SQL_DAO.deleteExpireExecute(expire);
    }

    @Override
    public void addRecoveryExecute(long executeId, TaskInfo info) throws NaviException {
        MY_SQL_JOB_EXECUTE_SQL_DAO.addRecoveryExecute(executeId, info);
    }

    @Override
    public void updateRecoveryExecute(long executeId, TaskInfo info) throws NaviException {
        MY_SQL_JOB_EXECUTE_SQL_DAO.updateRecoveryExecute(executeId, info);
    }

    @Override
    public void setRecoveryExecuteSuccess(TaskInfo info) throws NaviException {
        MY_SQL_JOB_EXECUTE_SQL_DAO.setRecoveryExecuteSuccess(info);
    }

    @Override
    public List<TaskInfo> getNotRecoveryTask() throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.getNotRecoveryTask();
    }

    @Override
    public List<RecoveryExecuteResult> queryRecoveryResultByScheduleId(String scheduleId, Long limit)
            throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.queryRecoveryResultByScheduleId(scheduleId, limit);
    }

    @Override
    public int getRecoveryExecuteTimes(String scheduleId, Long scheduleTime) throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.getRecoveryExecuteTimes(scheduleId, scheduleTime);
    }

    @Override
    public void deleteExpireRecovery(long expire) throws NaviException {
        MY_SQL_JOB_EXECUTE_SQL_DAO.deleteExpireRecovery(expire);
    }

    @Override
    public Map<String, Set<Long>> listRunnerExecute() throws NaviException {
        return MY_SQL_JOB_EXECUTE_SQL_DAO.listRunnerExecute();
    }

    @Override
    public void addEvent(TaskEventInfo eventInfo) throws NaviException {
        MY_SQL_JOB_EVENT_DAO.addEvent(eventInfo);
    }

    @Override
    public void updateEventResult(long eventId, TaskEventResult result) throws NaviException {
        MY_SQL_JOB_EVENT_DAO.updateEventResult(eventId, result);
    }

    @Override
    public TaskEventResult queryEventResult(long eventId) throws NaviException {
        return MY_SQL_JOB_EVENT_DAO.queryEventResult(eventId);
    }

    @Override
    public EventContext getEventContext(Long executeId) throws NaviException {
        return MY_SQL_JOB_EVENT_DAO.getEventContext(executeId);
    }

        @Override
    public long getCurrentEventIdIndex() throws NaviException {
        return MY_SQL_JOB_EVENT_DAO.getCurrentEventIdIndex();
    }

    @Override
    public List<TaskEventInfo> getNotProcessTaskEvents() throws NaviException {
        return MY_SQL_JOB_EVENT_DAO.getNotProcessTaskEvents();
    }

    @Override
    public void deleteExpireEvent(long expire) throws NaviException {
        MY_SQL_JOB_EVENT_DAO.deleteExpireEvent(expire);
    }

    //task type view
    @Override
    public void addTaskType(TaskType taskType)
            throws NaviException {
        MY_SQL_JOB_TASK_TYPE_DAO.addTaskType(taskType);
    }

    @Override
    public void deleteTaskType(String typeId, String typeTag) throws NaviException {
        MY_SQL_JOB_TASK_TYPE_DAO.deleteTaskType(typeId, typeTag);
    }

    @Override
    public Map<String, Map<String, TaskType>> listTaskType() throws NaviException {
        return MY_SQL_JOB_TASK_TYPE_DAO.listTaskType();
    }

    @Override
    public TaskType queryTaskType(String typeId, String typeTag, String nodeLabel) throws NaviException {
        return MY_SQL_JOB_TASK_TYPE_DAO.queryTaskType(typeId, typeTag, nodeLabel);
    }

    @Override
    public void addTaskTypeTagAlias(String typeId, String typeTag, String alias, String description)
            throws NaviException {
        MY_SQL_JOB_TASK_TYPE_DAO.addTaskTypeTagAlias(typeId, typeTag, alias, description);
    }

    @Override
    public void deleteTaskTypeTagAlias(String typeId, String typeTag, String alias) throws NaviException {
        MY_SQL_JOB_TASK_TYPE_DAO.deleteTaskTypeTagAlias(typeId, typeTag, alias);
    }

    @Override
    public List<String> queryTaskTypeTagAlias(String typeId, String typeTag) throws NaviException {
        return MY_SQL_JOB_TASK_TYPE_DAO.queryTaskTypeTagAlias(typeId, typeTag);
    }

    @Override
    public void addTaskTypeDefaultTag(String typeId, String nodeLabel, String defaultTag) throws NaviException {
        MY_SQL_JOB_TASK_TYPE_DAO.addTaskTypeDefaultTag(typeId, nodeLabel, defaultTag);
    }

    @Override
    public void deleteTaskTypeDefaultTag(String typeId, String nodeLabel) throws NaviException {
        MY_SQL_JOB_TASK_TYPE_DAO.deleteTaskTypeDefaultTag(typeId, nodeLabel);
    }

    @Override
    public Map<String, String> listTaskTypeDefaultTag() throws NaviException {
        return MY_SQL_JOB_TASK_TYPE_DAO.listTaskTypeDefaultTag();
    }

    @Override
    public String queryTaskTypeDefaultTag(String typeId, String nodeLabel) throws NaviException {
        return MY_SQL_JOB_TASK_TYPE_DAO.queryTaskTypeDefaultTag(typeId, nodeLabel);
    }

    //node label view
    @Override
    public void addNodeLabel(NodeLabel label) throws NaviException {
        MY_SQL_JOB_NODE_LABEL_DAO.addNodeLabel(label);
    }

    @Override
    public void deleteNodeLabel(String labelName) throws NaviException {
        MY_SQL_JOB_NODE_LABEL_DAO.deleteNodeLabel(labelName);
    }

    @Override
    public List<NodeLabel> listNodeLabel() throws NaviException {
        return MY_SQL_JOB_NODE_LABEL_DAO.listNodeLabel();
    }

    @Override
    public void addHostNodeLabel(NodeLabelHost labelHost) throws NaviException {
        MY_SQL_JOB_NODE_LABEL_DAO.addHostNodeLabel(labelHost);
    }

    @Override
    public void deleteHostLabel(String host, String labelName) throws NaviException {
        MY_SQL_JOB_NODE_LABEL_DAO.deleteHostLabel(host, labelName);
    }

    @Override
    public void deleteAllHostLabel(String host) throws NaviException {
        MY_SQL_JOB_NODE_LABEL_DAO.deleteAllHostLabel(host);
    }

    @Override
    public List<NodeLabelHost> listNodeLabelHost() throws NaviException {
        return MY_SQL_JOB_NODE_LABEL_DAO.listNodeLabelHost();
    }

    @Override
    public void saveSavepoint(String scheduleId, String savepointStr) throws NaviException {
        MY_SQL_JOB_SAVEPOINT_DAO.saveSavepoint(scheduleId, savepointStr);
    }

    @Override
    public void saveSavepoint(String scheduleId, Long scheduleTime, String savepointStr) throws NaviException {
        MY_SQL_JOB_SAVEPOINT_DAO.saveSavepoint(scheduleId, scheduleTime, savepointStr);
    }

    @Override
    public String getSavepoint(String scheduleId) throws NaviException {
        return MY_SQL_JOB_SAVEPOINT_DAO.getSavepoint(scheduleId);
    }

    @Override
    public String getSavepoint(String scheduleId, Long scheduleTime) throws NaviException {
        return MY_SQL_JOB_SAVEPOINT_DAO.getSavepoint(scheduleId, scheduleTime);
    }

    @Override
    public void deleteSavepoint(String scheduleId) throws NaviException {
        MY_SQL_JOB_SAVEPOINT_DAO.deleteSavepoint(scheduleId);
    }

    @Override
    public void deleteSavepoint(String scheduleId, Long scheduleTime) throws NaviException {
        MY_SQL_JOB_SAVEPOINT_DAO.deleteSavepoint(scheduleId, scheduleTime);
    }

    @Override
    public List<ExecuteStatusCache.ExecuteReference> listExecuteReference() throws NaviException {
        return MY_SQL_JOB_EXECUTE_STATUS_CACHE_DAO.listExecuteReference();
    }

    @Override
    public Map<String, Map<Long, TaskStatus>> listExecuteStatusCache() throws NaviException {
        return MY_SQL_JOB_EXECUTE_STATUS_CACHE_DAO.listExecuteStatusCache();
    }

    @Override
    public void saveExecuteReferenceCache(String scheduleId, long beginDataTime, long endDataTime,
            String childScheduleId, long childDataTime) throws NaviException {
        MY_SQL_JOB_EXECUTE_STATUS_CACHE_DAO
                .saveExecuteReferenceCache(scheduleId, beginDataTime, endDataTime, childScheduleId, childDataTime);
    }

    @Override
    public void updateExecuteReferenceCache(String childScheduleId, long childDataTime, Boolean isHot)
            throws NaviException {
        MY_SQL_JOB_EXECUTE_STATUS_CACHE_DAO.updateExecuteReferenceCache(childScheduleId, childDataTime, isHot);
    }

    @Override
    public void activateExecuteReferenceCache(String scheduleId, String childScheduleId, long childDataTime)
            throws NaviException {
        MY_SQL_JOB_EXECUTE_STATUS_CACHE_DAO
                .activateExecuteReferenceCache(scheduleId, childScheduleId, childDataTime);
    }

    @Override
    public void removeExecuteReferenceCache(String scheduleId, String childScheduleId, long childDataTime)
            throws NaviException {
        MY_SQL_JOB_EXECUTE_STATUS_CACHE_DAO.removeExecuteReferenceCache(scheduleId, childScheduleId, childDataTime);
    }

    private boolean isVersionTableExist() throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_VERSION_TABLE_EXIST);
            int index = 1;
            ps.setString(index, con.getCatalog());
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return false;
    }

    private void upgradeFull(String version) throws NaviException {
        String fileName = Constants.SQL_FLIE_MYSQL_FULL_PREFIX + version + ".sql";
        executeSqlFile(fileName);
    }

    private void upgradeDelta(String version, String dbVersion) throws NaviException {
        List<String> sqlVersionFiles = new ArrayList<>();
        try {
            URL sqlFileUrl = this.getClass().getResource(" /sql/");
            if (sqlFileUrl != null) {
                try (FileSystem fileSystem = FileSystems
                        .newFileSystem(sqlFileUrl.toURI(), Collections.<String, Object>emptyMap())) {
                    Path myPath = fileSystem.getPath(" /sql/");
                    DirectoryStream<Path> stream = Files.newDirectoryStream(myPath);
                    for (Path path : stream) {
                        String name = path.toString().substring(" /sql/".length());
                        if (name.startsWith(Constants.SQL_FILE_MYSQL_DELTA_PREFIX)) {
                            String sqlVersion = name.substring(Constants.SQL_FILE_MYSQL_DELTA_PREFIX.length(),
                                    (name.length() - ".sql".length()));
                            if (sqlVersion.compareTo(dbVersion) > 0 && sqlVersion.compareTo(version) <= 0) {
                                sqlVersionFiles.add(name);
                            }
                        }
                    }
                } catch (IOException e) {
                    LOGGER.error(e);
                }
                Collections.sort(sqlVersionFiles);
                for (String sqlVersionFile : sqlVersionFiles) {
                    LOGGER.info("execute sql file: " + sqlVersionFile);
                    executeSqlFile(sqlVersionFile);
                }
            }
        } catch (Exception e) {
            throw new NaviException(e);
        }
    }

    private void executeSqlFile(String fileName) throws NaviException {
        InputStream is = this.getClass().getResourceAsStream(" /sql/" + fileName);
        Connection con = null;
      try {
            String[] sqlArray = SqlFileParser.parse(is);
            con = ConnectionPool.getConnection();
            con.setAutoCommit(false);
            for (String sql : sqlArray) {
                LOGGER.info("execute sql: " + sql);
                PreparedStatement ps = null;
                try {
                    ps = con.prepareStatement(sql);
                    ps.execute();
                } finally {
                    MySqlUtil.close(ps);
                }
            }
            con.commit();
        } catch (IOException | SQLException e) {
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException e1) {
                throw new NaviException(e1);
            }
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con);
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    LOGGER.error("sql file close error. ", e);
                }
            }
        }
    }
}
