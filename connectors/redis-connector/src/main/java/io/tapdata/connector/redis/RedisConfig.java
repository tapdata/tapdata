package io.tapdata.connector.redis;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class RedisConfig {

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class);

    private String host;
    private int port;
    private String user;
    private String password;
    private String deploymentMode;
    private String sentinelName;
    private String database;
    private ArrayList<LinkedHashMap<String, Integer>> sentinelAddress;

    private String valueType = "List";
    private String keyExpression;
    private String valueData;
    private String valueJoinString;
    private String valueTransferredString = "";
    private long expireTime;
    private Boolean resetExpire;
    private String keyTableName;
    private Boolean listHead = true;
    private Boolean oneKey;

    public RedisConfig load(Map<String, Object> map) {
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

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public Boolean getResetExpire() {
        return resetExpire;
    }

    public void setResetExpire(Boolean resetExpire) {
        this.resetExpire = resetExpire;
    }

    public String getValueData() {
        return valueData;
    }

    public void setValueData(String valueData) {
        this.valueData = valueData;
    }

    public String getValueJoinString() {
        return valueJoinString;
    }

    public void setValueJoinString(String valueJoinString) {
        this.valueJoinString = valueJoinString;
    }

    public String getValueTransferredString() {
        return valueTransferredString;
    }

    public void setValueTransferredString(String valueTransferredString) {
        this.valueTransferredString = valueTransferredString;
    }

    public String getKeyTableName() {
        return keyTableName;
    }

    public void setKeyTableName(String keyTableName) {
        this.keyTableName = keyTableName;
    }

    public Boolean getListHead() {
        return listHead;
    }

    public void setListHead(Boolean listHead) {
        this.listHead = listHead;
    }

    public String getKeyExpression() {
        return keyExpression;
    }

    public void setKeyExpression(String keyExpression) {
        this.keyExpression = keyExpression;
    }

    public Boolean getOneKey() {
        return oneKey;
    }

    public void setOneKey(Boolean oneKey) {
        this.oneKey = oneKey;
    }

}
