package io.tapdata.oceanbase.util;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

/**
 * @author dayun
 * @date 2022/6/27 17:44
 */
public class ConnectionUtil {
    public static HikariDataSource getHikariDataSource(TapConnectionContext tapConnectionContext, String jdbcUrl) throws IllegalArgumentException {
        if (null == tapConnectionContext) {
            throw new IllegalArgumentException("TapConnectionContext cannot be null");
        }
        HikariDataSource hikariDataSource = new HikariDataSource();
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        hikariDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        String username = connectionConfig.getString("username");
        String password = connectionConfig.getString("password");
        hikariDataSource.setJdbcUrl(jdbcUrl);
        hikariDataSource.setUsername(username);
        hikariDataSource.setPassword(password);
        hikariDataSource.setMinimumIdle(1);
        hikariDataSource.setMaximumPoolSize(20);
        hikariDataSource.setAutoCommit(false);
        hikariDataSource.setIdleTimeout(60 * 1000L);
        hikariDataSource.setKeepaliveTime(60 * 1000L);
        return hikariDataSource;
    }
}
