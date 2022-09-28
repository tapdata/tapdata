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

public class FileConfig implements Serializable {

    private static final String TAG = FileConfig.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class); //json util
    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    private String fileType;
    private String protocolType;
    private String host;
    private int port;
    private String filePathString;
    private Set<String> filePathSet;
    private Boolean recursive;

    public String getConnectionString() {
        String connectionString = protocolType + "://";
        if (EmptyKit.isNotBlank(host)) {
            connectionString += host + ":" + port + "/";
        }
        if (EmptyKit.isNotBlank(filePathString)) {
            connectionString += filePathString;
        }
        return connectionString;
    }

    public FileConfig load(Map<String, Object> map) {
        beanUtils.mapToBean(map, this);
        if (EmptyKit.isNotBlank(filePathString)) {
            filePathSet = Arrays.stream(filePathString.split(",")).collect(Collectors.toSet());
        }
        return this;
    }

    public FileConfig load(String json) {
        try {
            beanUtils.copyProperties(jsonParser.fromJson(json, this.getClass()), this);
            if (EmptyKit.isNotBlank(filePathString)) {
                filePathSet = Arrays.stream(filePathString.split(",")).collect(Collectors.toSet());
            }
            return this;
        } catch (Exception e) {
            TapLogger.error(TAG, "config json file is invalid!");
            e.printStackTrace();
        }
        return null;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getProtocolType() {
        return protocolType;
    }

    public void setProtocolType(String protocolType) {
        this.protocolType = protocolType;
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

    public void setPort(int port) {
        this.port = port;
    }

    public String getFilePathString() {
        return filePathString;
    }

    public void setFilePathString(String filePathString) {
        this.filePathString = filePathString;
    }

    public Set<String> getFilePathSet() {
        return filePathSet;
    }

    public void setFilePathSet(Set<String> filePathSet) {
        this.filePathSet = filePathSet;
    }

    public Boolean getRecursive() {
        return recursive;
    }

    public void setRecursive(Boolean recursive) {
        this.recursive = recursive;
    }
}
