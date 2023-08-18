package io.tapdata.connector.redis;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.EmptyKit;
import redis.clients.jedis.HostAndPort;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisConfig {

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class);

    private String host;
    private int port;
    private String user;
    private String password;
    private String deploymentMode;
    private String sentinelName;
    private int database;
    private ArrayList<LinkedHashMap<String, Integer>> sentinelAddress;
    private List<HostAndPort> clusterNodes;

    private String valueType = "List";
    private String keyExpression;
    private String keyPrefix;
    private String keyJoin;
    private String keySuffix;
    private String valueData = "Text";
    private String valueJoinString = ",";
    private String valueTransferredString = "";
    private Boolean csvFormat = true;
    private long expireTime;
    private Boolean resetExpire;
    private Boolean listHead = true;
    private Boolean oneKey = false;
    private String schemaKey = "-schema-key-";

    public RedisConfig load(Map<String, Object> map) {
        beanUtils.mapToBean(map, this);
        if (EmptyKit.isNotNull(sentinelAddress)) {
            clusterNodes = sentinelAddress.stream().map(v ->
                    new HostAndPort(String.valueOf(v.get("host")), v.get("port"))).collect(Collectors.toList());
        }
        return this;
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

    public List<HostAndPort> getClusterNodes() {
        return clusterNodes;
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

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
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

    public Boolean getCsvFormat() {
        return csvFormat;
    }

    public void setCsvFormat(Boolean csvFormat) {
        this.csvFormat = csvFormat;
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

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public String getKeyJoin() {
        return keyJoin;
    }

    public void setKeyJoin(String keyJoin) {
        this.keyJoin = keyJoin;
    }

    public String getKeySuffix() {
        return keySuffix;
    }

    public void setKeySuffix(String keySuffix) {
        this.keySuffix = keySuffix;
    }

    public Boolean getOneKey() {
        return oneKey;
    }

    public void setOneKey(Boolean oneKey) {
        this.oneKey = oneKey;
    }

    public String getSchemaKey() {
        return schemaKey;
    }

    public void setSchemaKey(String schemaKey) {
        this.schemaKey = schemaKey;
    }
}
