package io.tapdata.common;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MqConfig implements Serializable {

    private static final String TAG = MqConfig.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class); //json util
    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    private String __connectionType;
    private String mqType;
    private String mqHost;
    private int mqPort;
    private String nameSrvAddr;
    private String mqUsername;
    private String mqPassword;
    private String mqTopicString;
    private Set<String> mqTopicSet;
    private String mqQueueString;
    private Set<String> mqQueueSet;

    public MqConfig load(Map<String, Object> map) {
        assert beanUtils != null;
        beanUtils.mapToBean(map, this);
        if (EmptyKit.isNotBlank(mqTopicString)) {
            mqTopicSet = Arrays.stream(mqTopicString.split(",")).collect(Collectors.toSet());
        }
        if (EmptyKit.isNotBlank(mqQueueString)) {
            mqQueueSet = Arrays.stream(mqQueueString.split(",")).collect(Collectors.toSet());
        }
        return this;
    }

    public String getConnectionString() {
        if (EmptyKit.isNotBlank(nameSrvAddr)) {
            return nameSrvAddr;
        } else {
            return mqHost + ":" + mqPort;
        }
    }

    public MqConfig load(String json) {
        try {
            assert beanUtils != null;
            assert jsonParser != null;
            beanUtils.copyProperties(jsonParser.fromJson(json, this.getClass()), this);
            if (EmptyKit.isNotBlank(mqTopicString)) {
                mqTopicSet = Arrays.stream(mqTopicString.split(",")).collect(Collectors.toSet());
            }
            if (EmptyKit.isNotBlank(mqQueueString)) {
                mqQueueSet = Arrays.stream(mqQueueString.split(",")).collect(Collectors.toSet());
            }
            return this;
        } catch (Exception e) {
            TapLogger.error(TAG, "config json file is invalid!");
            e.printStackTrace();
        }
        return null;
    }

    public String get__connectionType() {
        return __connectionType;
    }

    public void set__connectionType(String __connectionType) {
        this.__connectionType = __connectionType;
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

    public int getMqPort() {
        return mqPort;
    }

    public void setMqPort(int mqPort) {
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

    public Set<String> getMqTopicSet() {
        return mqTopicSet;
    }

    public void setMqTopicSet(Set<String> mqTopicSet) {
        this.mqTopicSet = mqTopicSet;
    }

    public String getMqQueueString() {
        return mqQueueString;
    }

    public void setMqQueueString(String mqQueueString) {
        this.mqQueueString = mqQueueString;
    }

    public Set<String> getMqQueueSet() {
        return mqQueueSet;
    }

    public void setMqQueueSet(Set<String> mqQueueSet) {
        this.mqQueueSet = mqQueueSet;
    }
}
