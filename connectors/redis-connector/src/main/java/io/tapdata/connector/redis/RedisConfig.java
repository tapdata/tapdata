package io.tapdata.connector.redis;

import io.tapdata.connector.constant.HostPort;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

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

    private String deployMode = "standalone";

    private String sentinelName;

    private List<HostPort> hostPorts;




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

    public String getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getSentinelName() {
        return sentinelName;
    }

    public void setSentinelName(String sentinelName) {
        this.sentinelName = sentinelName;
    }

    public List<HostPort> getHostPorts() {
        return hostPorts;
    }

    public void setHostPorts(List<HostPort> hostPorts) {
        this.hostPorts = hostPorts;
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



}
