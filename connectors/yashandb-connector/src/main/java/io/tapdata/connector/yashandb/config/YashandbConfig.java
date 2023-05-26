package io.tapdata.connector.yashandb.config;

import io.tapdata.common.CommonDbConfig;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Author:Skeet
 * Date: 2023/5/16
 **/

@Setter
@Getter
public class YashandbConfig extends CommonDbConfig implements Serializable {
    private String databaseUrlPattern = "jdbc:yashandb://%s:%s/%s";
    public String getDatabaseUrl() {
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase());
    }
    public YashandbConfig() {
        setDbType("yashandb");
        setJdbcDriver("com.yashandb.jdbc.Driver");
    }
}
