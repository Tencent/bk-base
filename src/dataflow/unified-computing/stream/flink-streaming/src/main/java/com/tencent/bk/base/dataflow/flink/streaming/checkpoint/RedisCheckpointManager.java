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

package com.tencent.bk.base.dataflow.flink.streaming.checkpoint;

import com.tencent.bk.base.dataflow.core.common.Tools;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.types.AbstractCheckpointKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

public class RedisCheckpointManager extends AbstractFlinkStreamingCheckpointManager {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCheckpointManager.class);
    private static final int SAMPLE_EXPIRE_FACTOR = 10;
    private static final int ONE_MONTH_TTL_SECONDS = 3600 * 24 * 31;

    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;
    private final int redisTimeout;
    // sentinel
    private final String masterName;
    private final Set<String> sentinels;

    private transient Pool<Jedis> jedisPool;
    //记录操作数量
    private long hashSetOpCount = 0;

    public RedisCheckpointManager(String redisHost, int redisPort, String redisPassword, int redisTimeout) {
        this(redisHost, redisPort, redisPassword, redisTimeout, null, null);
    }

    public RedisCheckpointManager(String redisHost, int redisPort, String redisPassword, int redisTimeout,
            String masterName, Set<String> sentinels) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.redisTimeout = redisTimeout;
        this.masterName = masterName;
        this.sentinels = sentinels;
        initRedisPool(this.redisHost, this.redisPort, this.redisPassword, this.redisTimeout, this.masterName,
                this.sentinels);
    }

    @Override
    public void open() {
        initRedisPool(this.redisHost, this.redisPort, this.redisPassword, this.redisTimeout, this.masterName,
                this.sentinels);
    }

    @Override
    public void savePoint(AbstractCheckpointKey checkpointKey, CheckpointValue.OutputCheckpoint checkpoint) {
        hsetHash(checkpointKey.getKey(), checkpointKey.getField(), checkpoint.toDbValue());
        //每10次hset操作后设置一次key过期时间:1个月
        sampleExpire(checkpointKey.getKey(), ONE_MONTH_TTL_SECONDS);
    }

    @Override
    public CheckpointValue.OutputCheckpoint getPoint(AbstractCheckpointKey checkpointKey) {
        LOGGER.info("Get check point by the checkpoint key " + checkpointKey.toString());
        String point = hgetHash(checkpointKey.getKey(), checkpointKey.getField());
        if (null == point) {
            return null;
        } else {
            return CheckpointValue.OutputCheckpoint.buildFromDbStr(point);
        }
    }

    /**
     * 通过node id获取对应的checkpoint
     *
     * @param nodeId node id
     * @return map
     */
    @Override
    public Map<Integer, CheckpointValue.OutputCheckpoint> listCheckpoints(String nodeId) {
        Map<Integer, CheckpointValue.OutputCheckpoint> checkpoints = new HashMap<>();

        for (Map.Entry<String, String> entry : hgetAllHash(nodeId).entrySet()) {
            checkpoints.put(Integer.valueOf(entry.getKey()),
                    CheckpointValue.OutputCheckpoint.buildFromDbStr(entry.getValue()));
        }
        LOGGER.info(String.format("List the node %s checkpoint is %s", nodeId, checkpoints.toString()));
        return checkpoints;
    }

    /**
     * 关闭redis连接池连接
     */
    @Override
    public void close() {
        try {
            if (jedisPool != null) {
                jedisPool.destroy();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to close redis pool.", e);
            // pass
        }
    }


    private void initRedisPool(String host, int port, String password, int timeout, String masterName,
            Set<String> sentinels) {
        if (jedisPool != null) {
            jedisPool.destroy();
        }
        for (int i = 0; i < 3; i++) {
            try {
                GenericObjectPoolConfig config = new GenericObjectPoolConfig();
                if (StringUtils.isNotBlank(masterName)
                        && null != sentinels && !sentinels.isEmpty()) {
                    //  Sentinel
                    jedisPool = new JedisSentinelPool(masterName, sentinels, config, timeout, password);
                } else {
                    jedisPool = new JedisPool(config, host, port, timeout, password);
                }
                return;
            } catch (Exception e) {
                LOGGER.warn(String.format("Failed to connect redis and retry, host: %s, port: %d", host, port));
                Tools.pause(i);
            }
        }
        throw new RuntimeException(String.format("Failed to connect redis, host: %s, port: %s", host, port));
    }

    private void hsetHash(String key, String field, String value) {
        Jedis jedis = null;
        for (int i = 0; i < 3; i++) {
            boolean retryConnect = false;
            try {
                jedis = jedisPool.getResource();
                jedis.hset(key, field, value);
                return;
            } catch (Exception e) {
                LOGGER.error(
                        String.format("failed to hset key %s field %s value %s and retry %d times!", key, field, value,
                                i), e);
                retryConnect = true;
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
                if (retryConnect) {
                    Tools.pause((int) (2 * Math.pow(2, i)));
                    initRedisPool(this.redisHost, this.redisPort, this.redisPassword, this.redisTimeout,
                            this.masterName, this.sentinels);
                }
            }
        }
        throw new RuntimeException(
                String.format("Failed to save checkpoint after three times, host: %s, port: %s", this.redisHost,
                        this.redisPort));
    }

    private String hgetHash(String key, String field) {
        Jedis jedis = null;
        for (int i = 0; i < 3; i++) {
            boolean retryConnect = false;
            try {
                jedis = jedisPool.getResource();
                String checkpoint = jedis.hget(key, field);
                if (null == checkpoint) {
                    return null;
                } else {
                    return checkpoint;
                }
            } catch (Exception e) {
                LOGGER.error(String.format("failed to hget key %s field %s, retry %d times!", key, field, i), e);
                retryConnect = true;
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
                if (retryConnect) {
                    Tools.pause((int) (2 * Math.pow(2, i)));
                    initRedisPool(this.redisHost, this.redisPort, this.redisPassword, this.redisTimeout,
                            this.masterName, this.sentinels);
                }
            }
        }
        throw new RuntimeException();
    }

    private void expire(String key, int seconds) {
        Jedis jedis = null;
        for (int i = 0; i < 3; i++) {
            boolean retryConnect = false;
            try {
                jedis = jedisPool.getResource();
                jedis.expire(key, seconds);
                return;
            } catch (Exception e) {
                LOGGER.error(String.format("failed to expire key %s and retry %d times!", key, i), e);
                retryConnect = true;
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
                if (retryConnect) {
                    Tools.pause((int) (2 * Math.pow(2, i)));
                    initRedisPool(this.redisHost, this.redisPort, this.redisPassword, this.redisTimeout,
                            this.masterName, this.sentinels);
                }
            }
        }
        throw new RuntimeException(
                String.format("Failed to expire checkpoint key after three times, host: %s, port: %s", this.redisHost,
                        this.redisPort));
    }

    /**
     * 防止每次hset操作都设置ttl显著增加redis每秒操作数
     * 故采用抽样方式设置ttl，每 SAMPLE_EXPIRE_FACTOR 次 hset 操作后设置一次ttl
     *
     * @param key 指定过期的key
     * @param seconds 指定过期时间
     */
    private void sampleExpire(String key, int seconds) {
        if (hashSetOpCount % SAMPLE_EXPIRE_FACTOR == 0) {
            expire(key, seconds);
        }
        ++hashSetOpCount;
    }

    private Map<String, String> hgetAllHash(String key) {
        Jedis jedis = null;
        for (int i = 0; i < 3; i++) {
            boolean retryConnect = false;
            try {
                jedis = jedisPool.getResource();
                return jedis.hgetAll(key);
            } catch (Exception e) {
                LOGGER.error(String.format("failed to hgetAll key %s, retry %d times!", key, i), e);
                retryConnect = true;
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
                if (retryConnect) {
                    Tools.pause((int) (2 * Math.pow(2, i)));
                    initRedisPool(this.redisHost, this.redisPort, this.redisPassword, this.redisTimeout,
                            this.masterName, this.sentinels);
                }
            }
        }
        throw new RuntimeException();
    }
}
