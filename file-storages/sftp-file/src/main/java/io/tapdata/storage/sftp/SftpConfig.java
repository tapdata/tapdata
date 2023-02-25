package io.tapdata.storage.sftp;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.io.Serializable;
import java.util.Map;

public class SftpConfig implements Serializable {

    private String sftpHost;
    private int sftpPort = 22;
    private String sftpUsername;
    private String sftpPassword;
    private String encoding;

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    public SftpConfig load(Map<String, Object> params) {
        assert beanUtils != null;
        beanUtils.mapToBean(params, this);
        return this;
    }

    public String getSftpHost() {
        return sftpHost;
    }

    public void setSftpHost(String sftpHost) {
        this.sftpHost = sftpHost;
    }

    public int getSftpPort() {
        return sftpPort;
    }

    public void setSftpPort(int sftpPort) {
        this.sftpPort = sftpPort;
    }

    public String getSftpUsername() {
        return sftpUsername;
    }

    public void setSftpUsername(String sftpUsername) {
        this.sftpUsername = sftpUsername;
    }

    public String getSftpPassword() {
        return sftpPassword;
    }

    public void setSftpPassword(String sftpPassword) {
        this.sftpPassword = sftpPassword;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
}
