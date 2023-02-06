package io.tapdata.connector.tablestore;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.EmptyKit;

import java.util.Map;
import java.util.StringJoiner;

public class TablestoreConfig {
    private static final BeanUtils BEAN_UTILS = InstanceFactory.instance(BeanUtils.class);

    private String endpoint;
    private String instance;
    private String accessKeyId;
    private String accessKeySecret;
    private String accessKeyToken;
    private String clientType;

    public TablestoreConfig load(Map<String, Object> map) {
        assert BEAN_UTILS != null;
        return BEAN_UTILS.mapToBean(map, this);
    }

    public String getConnectionString() {
        StringJoiner joiner = new StringJoiner("/");
        joiner.add(endpoint);
        joiner.add(instance);
        return joiner.toString();
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    public String getAccessKeyToken() {
        return accessKeyToken;
    }

    public void setAccessKeyToken(String accessKeyToken) {
        this.accessKeyToken = accessKeyToken;
    }

    public String getClientType() {
        return "NORMAL";
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }
}
