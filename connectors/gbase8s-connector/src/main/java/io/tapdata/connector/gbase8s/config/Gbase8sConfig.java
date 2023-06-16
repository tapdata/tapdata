package io.tapdata.connector.gbase8s.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

public class Gbase8sConfig extends CommonDbConfig implements Serializable {

    public Gbase8sConfig() {
        setDbType("gbasedbt-sqli");
        setJdbcDriver("com.gbasedbt.jdbc.Driver");
    }

}
