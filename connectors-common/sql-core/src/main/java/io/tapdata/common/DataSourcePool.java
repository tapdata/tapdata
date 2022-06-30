package io.tapdata.common;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.kit.EmptyKit;

import java.util.concurrent.ConcurrentHashMap;

public class DataSourcePool {

    private final static ConcurrentHashMap<String, JdbcContext> dataPool = new ConcurrentHashMap<>(16);

    /**
     * get jdbc context from database config
     *
     * @param config DatabaseConfig
     * @param clazz  Class<? extends JdbcContext>
     * @return jdbcContext
     */
    public static JdbcContext getJdbcContext(CommonDbConfig config, Class<? extends JdbcContext> clazz, String connectorId) {
        String key = uniqueKeyForDb(config);
        if (dataPool.containsKey(key) && dataPool.get(key).testValid()) {
            return dataPool.get(key).incrementConnector(connectorId);
        } else {
            JdbcContext context = null;
            try {
                context = clazz.getDeclaredConstructor(config.getClass(), HikariDataSource.class).newInstance(config, HikariConnection.getHikariDataSource(config));
                context.incrementConnector(connectorId);
                dataPool.put(key, context);
            } catch (Exception ignore) {
            }
            return context;
        }
    }

    /**
     * remove jdbcContext from pool
     *
     * @param config DatabaseConfig
     */
    public static void removeJdbcContext(CommonDbConfig config) {
        dataPool.remove(uniqueKeyForDb(config));
    }

    private static String uniqueKeyForDb(CommonDbConfig config) {
        if (EmptyKit.isNull(config)) {
            throw new RuntimeException("Data Source Error, Please check!");
        }
        return config.getHost() + config.getPort() + config.getDatabase() + config.getSchema();
    }

    //static Hikari connection
    static class HikariConnection {
        public static HikariDataSource getHikariDataSource(CommonDbConfig config) throws IllegalArgumentException {
            HikariDataSource hikariDataSource;
            if (EmptyKit.isNull(config)) {
                throw new IllegalArgumentException("Config cannot be null");
            }
            hikariDataSource = new HikariDataSource();
            //need 4 attributes
            hikariDataSource.setDriverClassName(config.getJdbcDriver());
            hikariDataSource.setJdbcUrl(config.getDatabaseUrl());
            hikariDataSource.setUsername(config.getUser());
            hikariDataSource.setPassword(config.getPassword());
            hikariDataSource.setMinimumIdle(1);
            hikariDataSource.setMaximumPoolSize(20);
            hikariDataSource.setAutoCommit(false);
            hikariDataSource.setIdleTimeout(60 * 1000L);
            hikariDataSource.setKeepaliveTime(60 * 1000L);
            return hikariDataSource;
        }
    }
}
