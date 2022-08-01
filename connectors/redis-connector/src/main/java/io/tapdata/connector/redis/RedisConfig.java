package io.tapdata.connector.redis;

import io.tapdata.connector.constant.HostPort;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lemon
 */
public class RedisConfig {

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class);

    private String host;
    private int port;
    private String user;
    private String password;

    private String deploymentMode ;

    private String sentinelName;

    private String database;

    private ArrayList<LinkedHashMap<String,Integer>> sentinelAddress;

    private String valueType;




    public RedisConfig load(Map<String, Object> map)  {
        return beanUtils.mapToBean(map, this);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public String getDeploymentMode() {
        return deploymentMode;
    }

    public void setDeploymentMode(String deploymentMode) {
        this.deploymentMode = deploymentMode;
    }

    public String getSentinelName() {
        return sentinelName;
    }

    public void setSentinelName(String sentinelName) {
        this.sentinelName = sentinelName;
    }

    public ArrayList<LinkedHashMap<String, Integer>> getSentinelAddress() {
        return sentinelAddress;
    }

    public void setSentinelAddress(ArrayList<LinkedHashMap<String, Integer>> sentinelAddress) {
        this.sentinelAddress = sentinelAddress;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }


    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }
}
