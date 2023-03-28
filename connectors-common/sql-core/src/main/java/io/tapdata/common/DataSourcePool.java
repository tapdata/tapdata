//package io.tapdata.common;
//
//import com.zaxxer.hikari.HikariDataSource;
//import io.tapdata.entity.logger.TapLogger;
//import io.tapdata.kit.EmptyKit;
//
//import java.util.concurrent.ConcurrentHashMap;
//
//public class DataSourcePool {
//
//    private final static String TAG = DataSourcePool.class.getSimpleName();
//
//    private final static ConcurrentHashMap<String, JdbcContext> dataPool = new ConcurrentHashMap<>(16);
//
//    /**
//     * get jdbc context from database config
//     *
//     * @param config DatabaseConfig
//     * @param clazz  Class<? extends JdbcContext>
//     * @return jdbcContext
//     */
//    public static JdbcContext getJdbcContext(CommonDbConfig config, Class<? extends JdbcContext> clazz, String connectorId) {
//        String key = uniqueKeyForDb(config);
//        synchronized (key.intern()) {
//            if (dataPool.containsKey(key) && dataPool.get(key).testValid()) {
//                TapLogger.info(TAG, "JdbcContext exists, reuse it");
//                return dataPool.get(key).incrementConnector(connectorId);
//            } else {
//                JdbcContext context = null;
//                try {
//                    TapLogger.info(TAG, "JdbcContext not exists, create it");
//                    context = clazz.getDeclaredConstructor(config.getClass(), HikariDataSource.class).newInstance(config, HikariConnection.getHikariDataSource(config));
//                    context.incrementConnector(connectorId);
//                    dataPool.put(key, context);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//                return context;
//            }
//        }
//    }
//
//    /**
//     * remove jdbcContext from pool
//     *
//     * @param config DatabaseConfig
//     */
//    public static void removeJdbcContext(CommonDbConfig config) {
//        final String uniqueKeyForDb = uniqueKeyForDb(config);
//        synchronized (uniqueKeyForDb.intern()) {
//            dataPool.remove(uniqueKeyForDb);
//        }
//    }
//
//    private static String uniqueKeyForDb(CommonDbConfig config) {
//        if (EmptyKit.isNull(config)) {
//            throw new RuntimeException("Data Source Error, Please check!");
//        }
//        return config.getHost() + config.getPort() + config.getDatabase() + config.getSchema();
//    }
//
//    //static Hikari connection
//    static class HikariConnection {
//        public static HikariDataSource getHikariDataSource(CommonDbConfig config) throws IllegalArgumentException {
//            HikariDataSource hikariDataSource;
//            if (EmptyKit.isNull(config)) {
//                throw new IllegalArgumentException("Config cannot be null");
//            }
//            hikariDataSource = new HikariDataSource();
//            //need 4 attributes
//            hikariDataSource.setDriverClassName(config.getJdbcDriver());
//            hikariDataSource.setJdbcUrl(config.getDatabaseUrl());
//            hikariDataSource.setUsername(config.getUser());
//            hikariDataSource.setPassword(config.getPassword());
//            if (EmptyKit.isNotNull(config.getProperties())) {
//                hikariDataSource.setDataSourceProperties(config.getProperties());
//            }
//            hikariDataSource.setMinimumIdle(1);
//            hikariDataSource.setMaximumPoolSize(100); //-1 may be not limited
//            hikariDataSource.setAutoCommit(false);
//            hikariDataSource.setIdleTimeout(60 * 1000L);
//            hikariDataSource.setKeepaliveTime(60 * 1000L);
//            return hikariDataSource;
//        }
//    }
//}
