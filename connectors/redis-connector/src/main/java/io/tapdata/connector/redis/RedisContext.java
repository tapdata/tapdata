package io.tapdata.connector.redis;

import io.tapdata.connector.constant.DeployModeEnum;
import io.tapdata.connector.constant.HostPort;
import io.tapdata.entity.logger.TapLogger;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.Pool;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author lemon
 */
public class RedisContext implements AutoCloseable {

    private static final String TAG = RedisContext.class.getSimpleName();
    private static final int MAX_TOTAL = 8;
    private static final int MAX_IDLE = 1;
    private static final int MAX_WAIT_MILLIS = 100 * 1000;
    private static final boolean TEST_ON_BORROW = true;
    private static final boolean TEST_ON_CREATE = false;
    private static final boolean TEST_ON_RETURN = false;
    private static final boolean TEST_WHILE_IDLE = false;

    private static final int POOL_TIMEOUT = 10 * 1000;

    private static final int GET_JEDIS_TIMEOUT_COUNT = 3;

    private  RedisConfig redisConfig;

    private Pool<Jedis> jedisPool;

    private final AtomicInteger dbNumber = new AtomicInteger(-1);

    public RedisContext(RedisConfig redisConfig) throws Exception {
        this.redisConfig = redisConfig;
        try{
            if (jedisPool == null) {
                synchronized (this) {
                    if (jedisPool == null) {
                        jedisPool = initializeJedisPool(redisConfig);
                    }
                }
            }
            initializeJedisPool(redisConfig);
        } catch (Exception e) {
            TapLogger.error(TAG, "close connection error", e);
            throw new Exception("Initial redis target failed %s", e);
        }
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

        DeployModeEnum deployModeEnum = DeployModeEnum.fromString(redisConfig.getDeployMode());
        if (deployModeEnum == null) {
            deployModeEnum = DeployModeEnum.STANDALONE;
        }

        if (deployModeEnum == DeployModeEnum.STANDALONE) {
            if (StringUtils.isNotBlank(redisConfig.getUser()) && StringUtils.isNotBlank(redisConfig.getPassword())) {
                jedisPool = new JedisPool(jedisPoolConfig, redisConfig.getHost(), redisConfig.getPort(), POOL_TIMEOUT, redisConfig.getPassword());
            } else {
                jedisPool = new JedisPool(jedisPoolConfig, redisConfig.getHost(), redisConfig.getPort(), POOL_TIMEOUT);
            }
        } else if (deployModeEnum == DeployModeEnum.SENTINEL) {
            final List<HostPort> hostPorts = redisConfig.getHostPorts();
            Set<String> sentinelHostPort = new HashSet<>(hostPorts.size());
            for (HostPort hostPort : hostPorts) {
                sentinelHostPort.add(hostPort.getHost() + ":" + hostPort.getPort());
            }
            if (StringUtils.isNotBlank(redisConfig.getUser()) && StringUtils.isNotBlank(redisConfig.getPassword())) {
                jedisPool = new JedisSentinelPool(
                        redisConfig.getSentinelName(),
                        sentinelHostPort,
                        jedisPoolConfig,
                        POOL_TIMEOUT,
                        redisConfig.getPassword()
                );
            } else {
                jedisPool = new JedisSentinelPool(
                        redisConfig.getSentinelName(),
                        sentinelHostPort,
                        jedisPoolConfig,
                        POOL_TIMEOUT
                );
            }
        }
        return jedisPool;
    }

    public Jedis getJedis() {
        Jedis jedis = null;
        int retryCount = 0;
        boolean jedisException = false;
        while (jedisException) {
            try {
                jedis = jedisPool.getResource();
                if (StringUtils.isNotBlank(redisConfig.getUser())) {
                    jedis.select(Integer.parseInt(redisConfig.getUser()));
                }
                return jedis;
            } catch (Exception e) {
                if (e instanceof JedisConnectionException) {
                    retryCount++;
                    jedisException = true;
                    if (retryCount > GET_JEDIS_TIMEOUT_COUNT) {
                        break;
                    }
                    TapLogger.warn("Getjedis failed,Try again {} times,retry count: {}", String.valueOf(GET_JEDIS_TIMEOUT_COUNT), retryCount);
                } else {
                    TapLogger.error("Getjedis error,message: {}", e.getMessage(), e);
                    break;
                }
            }
        }

        return jedis;
    }




    public RedisConfig getRedisConfig() {
        return redisConfig;
    }

    public Pool<Jedis> getJedisPool() {
        return jedisPool;
    }

    @Override
    public void close() throws Exception {
        jedisPool.close();
    }
}
