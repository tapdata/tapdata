package com.tapdata.tm.modules.vo;

import lombok.Data;

@Data
public class ConnectionVo {
    private String name;
    private String connection_type;
    private String database_type;
    private String database_host;
    private String database_username;
    private Integer database_port;
    private String database_owner;
    private String database_uri;
    private String database_name;
    private String database_password;
    private Integer retry;
    private Object nextRetry;
    private String id;
    private Boolean ssl;
    private String sslKey;
    private String sslPass;
    private Boolean sslValidate;
    private String sslCA;
    private String sslCert;
    private Boolean enableArithAbort;
}
