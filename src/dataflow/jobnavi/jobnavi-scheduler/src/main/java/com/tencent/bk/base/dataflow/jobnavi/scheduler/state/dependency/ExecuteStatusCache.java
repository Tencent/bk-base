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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;

public class ExecuteStatusCache {

    private static final Logger logger = Logger.getLogger(ExecuteStatusCache.class);

    private static final String KEY_SEPARATOR = "@";

    //key: scheduleId->dataTime, value: status
    private static final Map<String, Map<Long, TaskStatus>> statusCache = new HashMap<>();
    //key: scheduleId->dataTime, value: reference count
    private static final Map<String, SortedMap<Long, Integer>> statusReferenceCountMap = new HashMap<>();
    //key: childScheduleId@childDataTime@scheduleId, value: execute reference
    private static final Map<String, ExecuteReference> hotExecuteReference = new LinkedHashMap<>();
    //key: childScheduleId@childDataTime, value: set(childScheduleId@childDataTime@scheduleId)
    private static final Map<String, Set<String>> executeReferenceKeyIndex = new HashMap<>();
    //key: childScheduleId@childDataTime@scheduleId, value: execute reference
    private static final Map<String, ExecuteReference> coldExecuteReference = new LinkedHashMap<>();
    private static final Object syncLock = new Object();
    private static final BlockingQueue<ExecuteReference> removeReferenceQueue = new LinkedBlockingQueue<>();
    private static Thread clearCacheThread;

    private static AbstractJobDao jobDao;
    private static int maxCapacity = Integer.MAX_VALUE;

    public static void init(Configuration conf) {
        setMaxCapacity(conf.getInt(Constants.JOBNAVI_SCHEDULER_TASK_EXECUTE_CACHE_CAPACITY_MAX,
                Constants.JOBNAVI_SCHEDULER_TASK_EXECUTE_CACHE_CAPACITY_MAX_DEFAULT));
        clearCacheThread = new Thread(new ClearCacheThread(), "clear-execute-cache-thread");
        clearCacheThread.start();
    }

    public static void destroy() {
        if (clearCacheThread != null) {
            clearCacheThread.interrupt();
        }
        synchronized (syncLock) {
            statusCache.clear();
            statusReferenceCountMap.clear();
            hotExecuteReference.clear();
            executeReferenceKeyIndex.clear();
            coldExecuteReference.clear();
            removeReferenceQueue.clear();
        }
    }

    /**
     * rebuild cache from DB
     *
     * @param jobDao DB dao
     * @throws NaviException
     */
    public static void rebuild(AbstractJobDao jobDao) throws NaviException {
        ExecuteStatusCache.jobDao = jobDao;
        //rebuild reference cache from DB
        List<ExecuteReference> executeReferences = jobDao.listExecuteReference();
        for (ExecuteReference reference : executeReferences) {
            String scheduleId = reference.getScheduleId();
            long beginDataTime = reference.getBeginDataTime();
            long endDataTime = reference.getEndDataTime();
            String childScheduleId = reference.getChildScheduleId();
            long childDataTime = reference.getChildDataTime();
            putReferenceLocal(scheduleId, beginDataTime, endDataTime, childScheduleId, childDataTime);
            if (!reference.isHot()) {
                removeReferenceLocal(childScheduleId, childDataTime); //rebuild cold reference cache
            }
        }
        //rebuild execute status cache
        Map<String, Map<Long, TaskStatus>> statusCache = jobDao.listExecuteStatusCache();
        for (Map.Entry<String, Map<Long, TaskStatus>> statusCacheEntry : statusCache.entrySet()) {
            String scheduleId = statusCacheEntry.getKey();
            for (Map.Entry<Long, TaskStatus> entry : statusCacheEntry.getValue().entrySet()) {
                long dataTime = entry.getKey();
                TaskStatus status = entry.getValue();
                putStatus(scheduleId, dataTime, status);
            }
        }
        logger.info(String.format("reload %d execute references from DB", size()));
    }

    /**
     * set max capacity of cache, cold references will be removed if size of cache exceed max capacity
     *
     * @param maxCapacity max capacity
     */
    public static void setMaxCapacity(int maxCapacity) {
        ExecuteStatusCache.maxCapacity = maxCapacity;
    }

    public static int size() {
        synchronized (syncLock) {
            return hotExecuteReference.size() + coldExecuteReference.size();
        }
    }

    public static TaskStatus getStatus(String scheduleId, long dataTime) {
        synchronized (syncLock) {
            TaskStatus status = statusCache.get(scheduleId) != null ? statusCache.get(scheduleId).get(dataTime)
                    : TaskStatus.none;
            return status != null ? status : TaskStatus.none;
        }
    }

    /**
     * put execute status into local cache
     *
     * @param scheduleId schedule ID
     * @param dataTime data time
     * @param taskStatus execute status
     */
    public static void putStatus(String scheduleId,
            long dataTime,
            TaskStatus taskStatus) {
        synchronized (syncLock) {
            if (!statusCache.containsKey(scheduleId)) {
                statusCache.put(scheduleId, new HashMap<Long, TaskStatus>());
            }
            statusCache.get(scheduleId).put(dataTime, taskStatus);
            logger.info(String.format("put execute status into local cache:%s@%d[%s]", scheduleId, dataTime,
                    taskStatus.toString()));
        }
    }

    /**
     * put execute status cache with referring child execute info
     *
     * @param scheduleId schedule ID
     * @param dataTime data time
     * @param childScheduleId referring child execute schedule ID
     * @param childDataTime referring child execute schedule time
     */
    public static void putReference(String scheduleId,
            long dataTime,
            String childScheduleId,
            long childScheduleTime,
            long childDataTime) {
        synchronized (syncLock) {
            if (!statusCache.containsKey(scheduleId) || !statusCache.get(scheduleId).containsKey(dataTime)) {
                return;
            }
        }
        Long[] dependParentDataTimeRange = DependencyManager
                .getDependParentDataTimeRange(childScheduleId, childScheduleTime, scheduleId);
        if (dependParentDataTimeRange != null) {
            long beginDataTime = dependParentDataTimeRange[0];
            long endDataTime = dependParentDataTimeRange[1];
            putReferenceRemote(scheduleId, beginDataTime, endDataTime, childScheduleId, childDataTime);
            if (putReferenceLocal(scheduleId, beginDataTime, endDataTime, childScheduleId, childDataTime)) {
                synchronized (syncLock) {
                    //update reference count of execute in given range
                    if (!statusReferenceCountMap.containsKey(scheduleId)) {
                        statusReferenceCountMap.put(scheduleId, new TreeMap<Long, Integer>());
                    }
                    if (!statusReferenceCountMap.get(scheduleId).containsKey(dataTime)) {
                        statusReferenceCountMap.get(scheduleId).put(dataTime, 0);
                    }
                    //increase execute reference count
                    Integer referenceCount = statusReferenceCountMap.get(scheduleId).get(dataTime);
                    statusReferenceCountMap.get(scheduleId).put(dataTime, referenceCount + 1);
                }
                removeExcessReferenceLocal();
            }
        }
    }

    /**
     * update cached execute status
     *
     * @param scheduleId cached schedule ID
     * @param dataTime cached data time
     * @param taskStatus cached execute status
     */
    public static void updateStatus(String scheduleId,
            long dataTime,
            TaskStatus taskStatus) {
        synchronized (syncLock) {
            if (statusCache.containsKey(scheduleId) && statusCache.get(scheduleId).containsKey(dataTime)) {
                statusCache.get(scheduleId).put(dataTime, taskStatus);
                logger.info(String.format("update execute status in local cache:%s@%d[%s]", scheduleId, dataTime,
                        taskStatus.toString()));
            }
        }
    }

    /**
     * move references by given child execute info to cold cache buffer
     *
     * @param childScheduleId referring child schedule ID
     * @param childDataTime referring child execute data time
     */
    public static void removeReference(String childScheduleId, long childDataTime) {
        if (removeReferenceLocal(childScheduleId, childDataTime) > 0) {
            removeReferenceRemote(childScheduleId, childDataTime);
            logger.info(String.format("move references of child execute:%s@%d into cold cache buffer", childScheduleId,
                    childDataTime));
        }
    }

    /**
     * put execute status cache with referring child execute info into memory
     *
     * @param scheduleId schedule ID
     * @param beginDataTime data time range begin
     * @param endDataTime data time range end
     * @param childScheduleId referring child execute schedule ID
     * @param childDataTime referring child execute data time
     * @return true if any new reference is put into local cache
     */
    private static boolean putReferenceLocal(String scheduleId,
            long beginDataTime,
            long endDataTime,
            String childScheduleId,
            long childDataTime) {
        synchronized (syncLock) {
            String executeReferenceKey = childScheduleId + KEY_SEPARATOR + childDataTime + KEY_SEPARATOR + scheduleId;
            if (hotExecuteReference.containsKey(executeReferenceKey)) {
                ExecuteReference executeReference = hotExecuteReference.get(executeReferenceKey);
                hotExecuteReference.remove(executeReferenceKey);
                //put this execute reference to the end of cache
                hotExecuteReference.put(executeReferenceKey, executeReference);
                logger.info(String.format("update accessed time of execute reference:%s@[%d,%d)->%s@%s in local cache",
                        scheduleId, beginDataTime, endDataTime, childScheduleId, childDataTime));
                return false;
            } else if (coldExecuteReference.containsKey(executeReferenceKey)) {
                return false;
            } else {
                //put into hot cache buffer
                ExecuteReference executeReference = new ExecuteReference(scheduleId, beginDataTime, endDataTime,
                        childScheduleId, childDataTime);
                executeReference.setHot(true);
                hotExecuteReference.put(executeReferenceKey, executeReference);
            }
            String childExecuteKey = childScheduleId + KEY_SEPARATOR + childDataTime;
            if (!executeReferenceKeyIndex.containsKey(childExecuteKey)) {
                executeReferenceKeyIndex.put(childExecuteKey, new LinkedHashSet<String>());
            }
            executeReferenceKeyIndex.get(childExecuteKey).add(executeReferenceKey);
            logger.info(String.format("put execute reference into local cache:%s@[%d,%d)->%s@%s",
                    scheduleId, beginDataTime, endDataTime, childScheduleId, childDataTime));
            return true;
        }
    }

    /**
     * put execute reference cache into DB
     *
     * @param scheduleId schedule ID
     * @param beginDataTime data time range begin
     * @param endDataTime data time range end
     * @param childScheduleId child schedule ID
     * @param childDataTime child task data time
     */
    private static void putReferenceRemote(String scheduleId,
            long beginDataTime,
            long endDataTime,
            String childScheduleId,
            long childDataTime) {
        try {
            String referenceKey = childScheduleId + KEY_SEPARATOR + childDataTime + KEY_SEPARATOR + scheduleId;
            synchronized (syncLock) {
                if (jobDao != null) {
                    if (hotExecuteReference.containsKey(referenceKey)) {
                        jobDao.activateExecuteReferenceCache(scheduleId, childScheduleId, childDataTime);
                    } else if (!coldExecuteReference.containsKey(referenceKey)) {
                        jobDao.saveExecuteReferenceCache(scheduleId, beginDataTime, endDataTime, childScheduleId,
                                childDataTime);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn(String.format("Failed to save execute reference cache:%s~%s~%d", scheduleId, childScheduleId,
                    childDataTime), e);
        }
    }

    /**
     * remove cold references if size of cache exceed max capacity
     */
    private static void removeExcessReferenceLocal() {
        synchronized (syncLock) {
            int countToRemove = hotExecuteReference.size() + coldExecuteReference.size() - maxCapacity;
            while (countToRemove > 0) {
                logger.info(String.format("remain %d execute references to remove", countToRemove));
                Iterator<Map.Entry<String, ExecuteReference>> iterator;
                ExecuteReference executeReference;
                if (coldExecuteReference.size() > 0 && coldExecuteReference.entrySet().iterator().hasNext()) {
                    //remove from cold references first
                    iterator = coldExecuteReference.entrySet().iterator();
                    executeReference = iterator.next().getValue();
                    //move redundant hot reference to cold cache
                    removeReference(executeReference.getChildScheduleId(), executeReference.getChildDataTime());
                } else if (hotExecuteReference.size() > 0 && hotExecuteReference.entrySet().iterator().hasNext()) {
                    iterator = hotExecuteReference.entrySet().iterator();
                    Map.Entry<String, ExecuteReference> eldestEntry = iterator.next();
                    executeReference = eldestEntry.getValue();
                    //remove execute reference key index
                    String childExecuteKey = executeReference.getChildScheduleId() + KEY_SEPARATOR + executeReference
                            .getChildDataTime();
                    if (executeReferenceKeyIndex.containsKey(childExecuteKey)) {
                        String executeReferenceKey = eldestEntry.getKey();
                        executeReferenceKeyIndex.get(childExecuteKey).remove(executeReferenceKey);
                        if (executeReferenceKeyIndex.get(childExecuteKey).size() <= 0) {
                            executeReferenceKeyIndex.remove(childExecuteKey);
                        }
                    }
                } else {
                    break;
                }
                //decrease status reference count
                String scheduleId = executeReference.getScheduleId();
                long beginDataTime = executeReference.getBeginDataTime();
                long endDataTime = executeReference.getEndDataTime();
                logger.info(String.format("removed execute reference from local cache:%s@[%d,%d)->%s@%s",
                        scheduleId, beginDataTime, endDataTime, executeReference.getChildScheduleId(),
                        executeReference.getChildDataTime()));
                if (statusReferenceCountMap.containsKey(scheduleId)) {
                    SortedMap<Long, Integer> referenceCountMap = statusReferenceCountMap.get(scheduleId)
                            .subMap(beginDataTime, endDataTime);
                    Iterator<Map.Entry<Long, Integer>> referenceCountIterator = referenceCountMap.entrySet().iterator();
                    while (referenceCountIterator.hasNext()) {
                        Map.Entry<Long, Integer> entry = referenceCountIterator.next();
                        int referenceCount = entry.getValue() != null ? entry.getValue() : 0;
                        long dataTime = entry.getKey();
                        if (--referenceCount <= 0) {
                            referenceCountIterator.remove();
                            if (statusCache.containsKey(scheduleId)) {
                                TaskStatus statusToRemove = statusCache.get(scheduleId).get(dataTime);
                                statusCache.get(scheduleId).remove(dataTime);
                                logger.info(
                                        String.format("removed execute status from local cache:%s@%d[%s]", scheduleId,
                                                dataTime, statusToRemove.toString()));
                            }
                        } else {
                            entry.setValue(referenceCount);
                        }
                    }
                    if (statusReferenceCountMap.get(scheduleId).size() <= 0) {
                        statusReferenceCountMap.remove(scheduleId);
                    }
                }
                removeReferenceQueue.add(executeReference);
                iterator.remove();
                --countToRemove;
            }
        }
    }

    private static int removeReferenceLocal(String childScheduleId, long childDataTime) {
        synchronized (syncLock) {
            String childExecuteKey = childScheduleId + KEY_SEPARATOR + childDataTime;
            Set<String> executeReferenceKeys = executeReferenceKeyIndex.get(childExecuteKey);
            int removedCount = 0;
            if (executeReferenceKeys != null) {
                for (String executeReferenceKey : executeReferenceKeys) {
                    ExecuteReference executeReference = hotExecuteReference.remove(executeReferenceKey);
                    //move execute reference from hot cache to cold cache buffer
                    if (executeReference != null) {
                        executeReference.setHot(false);
                        coldExecuteReference.put(executeReferenceKey, executeReference);
                        ++removedCount;
                    }
                }
                executeReferenceKeyIndex.remove(childExecuteKey);
            }
            return removedCount;
        }
    }

    private static void removeReferenceRemote(String childScheduleId, long childDataTime) {
        try {
            if (jobDao != null) {
                jobDao.updateExecuteReferenceCache(childScheduleId, childDataTime, false);
            }
        } catch (Exception e) {
            logger.warn(String.format("Failed to remove reference cache of child execute:%s~%d", childScheduleId,
                    childDataTime));
        }
    }

    public static class ExecuteReference {

        private final String scheduleId;
        private final long beginDataTime;
        private final long endDataTime;
        private final String childScheduleId;
        private final long childDataTime;
        private boolean isHot;

        public ExecuteReference(String scheduleId, long beginDataTime, long endDataTime, String childScheduleId,
                long childDataTime) {
            this.scheduleId = scheduleId;
            this.beginDataTime = beginDataTime;
            this.endDataTime = endDataTime;
            this.childScheduleId = childScheduleId;
            this.childDataTime = childDataTime;
        }

        public String getScheduleId() {
            return scheduleId;
        }

        public long getBeginDataTime() {
            return beginDataTime;
        }

        public long getEndDataTime() {
            return endDataTime;
        }

        public String getChildScheduleId() {
            return childScheduleId;
        }

        public long getChildDataTime() {
            return childDataTime;
        }

        public boolean isHot() {
            return isHot;
        }

        public void setHot(boolean hot) {
            isHot = hot;
        }
    }

    static class ClearCacheThread implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                ExecuteReference referenceToRemove;
                try {
                    referenceToRemove = removeReferenceQueue.take();
                    if (jobDao != null) {
                        //remove execute reference cache from DB
                        String scheduleId = referenceToRemove.getScheduleId();
                        String childScheduleId = referenceToRemove.getChildScheduleId();
                        long childDataTime = referenceToRemove.getChildDataTime();
                        jobDao.removeExecuteReferenceCache(scheduleId, childScheduleId, childDataTime);
                        logger.info(
                                String.format("removed execute reference from DB:%s@[%d,%d)->%s@%s",
                                        referenceToRemove.getScheduleId(), referenceToRemove.getBeginDataTime(),
                                        referenceToRemove.getEndDataTime(),
                                        referenceToRemove.getChildScheduleId(), referenceToRemove.getChildDataTime()));
                    }
                } catch (InterruptedException e) {
                    logger.warn("clear execute execute cache thread interrupt.");
                } catch (Throwable e) {
                    logger.error("clear execute execute cache error.", e);
                }
            }
        }
    }
}
