package io.tapdata.connector.redis;

import io.tapdata.connector.redis.constant.DeployModeEnum;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.Pool;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 * @author lemon
 */
public class RedisContext implements AutoCloseable {

    private static final String TAG = RedisContext.class.getSimpleName();
    private static final int MAX_TOTAL = 100;
    private static final int MAX_IDLE = 20;
    private static final int MAX_WAIT_MILLIS = -1;
    private static final boolean TEST_ON_BORROW = true;
    private static final boolean TEST_ON_CREATE = false;
    private static final boolean TEST_ON_RETURN = false;
    private static final boolean TEST_WHILE_IDLE = false;
    private static final int POOL_TIMEOUT = 10 * 1000;
    private static final int GET_JEDIS_TIMEOUT_COUNT = 3;

    private final RedisConfig redisConfig;
    private volatile Pool<Jedis> jedisPool;
    private volatile UnifiedJedis jedisCluster;

    public RedisContext(RedisConfig redisConfig) throws Exception {
        this.redisConfig = redisConfig;
        try {
            synchronized (this) {
                if (DeployModeEnum.fromString(redisConfig.getDeploymentMode()) == DeployModeEnum.CLUSTER) {
                    if (jedisCluster == null) {
                        jedisCluster = initializeJedisCluster(redisConfig);
                    }
                } else {
                    if (jedisPool == null) {
                        jedisPool = initializeJedisPool(redisConfig);
                    }
                }
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "close connection error", e);
            throw new Exception("Initial redis target failed %s", e);
        }
    }

    public static JedisCluster initializeJedisCluster(RedisConfig redisConfig) {
        ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig();
        connectionPoolConfig.setMaxTotal(MAX_TOTAL);
        connectionPoolConfig.setMaxIdle(MAX_IDLE);
        connectionPoolConfig.setMaxWaitMillis(MAX_WAIT_MILLIS);
        connectionPoolConfig.setTestOnBorrow(TEST_ON_BORROW);
        connectionPoolConfig.setTestOnCreate(TEST_ON_CREATE);
        connectionPoolConfig.setTestOnReturn(TEST_ON_RETURN);
        connectionPoolConfig.setTestWhileIdle(TEST_WHILE_IDLE);
        return new JedisCluster(new HashSet<>(redisConfig.getClusterNodes()), 5000, 5000, 10, redisConfig.getPassword(), redisConfig.getSentinelName(), connectionPoolConfig);
    }

    public static Pool<Jedis> initializeJedisPool(RedisConfig redisConfig) {
        Pool<Jedis> jedisPool = null;
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(MAX_TOTAL);
        jedisPoolConfig.setMaxIdle(MAX_IDLE);
        jedisPoolConfig.setMaxWaitMillis(MAX_WAIT_MILLIS);
        jedisPoolConfig.setTestOnBorrow(TEST_ON_BORROW);
        jedisPoolConfig.setTestOnCreate(TEST_ON_CREATE);
        jedisPoolConfig.setTestOnReturn(TEST_ON_RETURN);
        jedisPoolConfig.setTestWhileIdle(TEST_WHILE_IDLE);

        DeployModeEnum deployModeEnum = DeployModeEnum.fromString(redisConfig.getDeploymentMode());
        if (deployModeEnum == null) {
            deployModeEnum = DeployModeEnum.STANDALONE;
        }

        if (deployModeEnum == DeployModeEnum.STANDALONE) {
            if (StringUtils.isNotBlank(redisConfig.getPassword())) {
                jedisPool = new JedisPool(jedisPoolConfig, redisConfig.getHost(), redisConfig.getPort(), POOL_TIMEOUT, redisConfig.getPassword());
            } else {
                jedisPool = new JedisPool(jedisPoolConfig, redisConfig.getHost(), redisConfig.getPort(), POOL_TIMEOUT);
            }
        } else if (deployModeEnum == DeployModeEnum.SENTINEL) {
            final List<LinkedHashMap<String, Integer>> hostPorts = redisConfig.getSentinelAddress();
            Set<String> sentinelHostPort = new HashSet<>(hostPorts.size());
            for (LinkedHashMap<String, Integer> hostPort : hostPorts) {
                sentinelHostPort.add(hostPort.get("host") + ":" + hostPort.get("port"));
            }
            if (StringUtils.isNotBlank(redisConfig.getPassword())) {
                jedisPool = new JedisSentinelPool(
                        redisConfig.getSentinelName(), sentinelHostPort, jedisPoolConfig, POOL_TIMEOUT, redisConfig.getPassword());
            } else {
                jedisPool = new JedisSentinelPool(
                        redisConfig.getSentinelName(), sentinelHostPort, jedisPoolConfig, POOL_TIMEOUT);
            }
        }
        return jedisPool;
    }


    public CommonJedis getJedis() {
        if (DeployModeEnum.fromString(redisConfig.getDeploymentMode()) == DeployModeEnum.CLUSTER) {
            return new CommonJedis(jedisCluster);
        }
        Jedis jedis = null;
        int retryCount = 0;
        while (true) {
            try {
                jedis = jedisPool.getResource();
                jedis.select(redisConfig.getDatabase());
                return new CommonJedis(jedis);
            } catch (Exception e) {
                if (e instanceof JedisConnectionException) {
                    retryCount++;

                    if (retryCount > GET_JEDIS_TIMEOUT_COUNT) {
                        break;
                    }
                    TapLogger.warn("Get jedis failed,Try again {} times,retry count: {}", String.valueOf(GET_JEDIS_TIMEOUT_COUNT), retryCount);
                } else {
                    TapLogger.error("Get jedis error,message: {}", e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
        }
        return new CommonJedis(jedis);
    }

    public RedisConfig getRedisConfig() {
        return redisConfig;
    }

    public void close() {
        if (EmptyKit.isNotNull(jedisPool)) {
            jedisPool.close();
        }
        if (EmptyKit.isNotNull(jedisCluster)) {
            jedisCluster.close();
        }
    }
}
