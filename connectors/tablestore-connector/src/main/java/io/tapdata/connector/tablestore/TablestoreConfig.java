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
    private String id;
    private String key;
    private String token;
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getClientType() {
        return "NORMAL";
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }
}
