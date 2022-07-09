package io.tapdata.common;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;

import java.io.Serializable;
import java.util.Map;

public class MqConfig implements Serializable {

    private static final String TAG = MqConfig.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class); //json util
    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    private String mqType;
    private String mqHost;
    private Integer mqPort;
    private String nameSrvAddr;
    private String mqUsername;
    private String mqPassword;
    private String mqTopicString;
    private String mqQueueString;

    public MqConfig load(Map<String, Object> map) {
        return beanUtils.mapToBean(map, this);
    }

    public MqConfig load(String json) {
        try {
            beanUtils.copyProperties(jsonParser.fromJson(json, this.getClass()), this);
            return this;
        } catch (Exception e) {
            TapLogger.error(TAG, "config json file is invalid!");
            e.printStackTrace();
        }
        return null;
    }

    public String getMqType() {
        return mqType;
    }

    public void setMqType(String mqType) {
        this.mqType = mqType;
    }

    public String getMqHost() {
        return mqHost;
    }

    public void setMqHost(String mqHost) {
        this.mqHost = mqHost;
    }

    public Integer getMqPort() {
        return mqPort;
    }

    public void setMqPort(Integer mqPort) {
        this.mqPort = mqPort;
    }

    public String getNameSrvAddr() {
        return nameSrvAddr;
    }

    public void setNameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
    }

    public String getMqUsername() {
        return mqUsername;
    }

    public void setMqUsername(String mqUsername) {
        this.mqUsername = mqUsername;
    }

    public String getMqPassword() {
        return mqPassword;
    }

    public void setMqPassword(String mqPassword) {
        this.mqPassword = mqPassword;
    }

    public String getMqTopicString() {
        return mqTopicString;
    }

    public void setMqTopicString(String mqTopicString) {
        this.mqTopicString = mqTopicString;
    }

    public String getMqQueueString() {
        return mqQueueString;
    }

    public void setMqQueueString(String mqQueueString) {
        this.mqQueueString = mqQueueString;
    }
}
