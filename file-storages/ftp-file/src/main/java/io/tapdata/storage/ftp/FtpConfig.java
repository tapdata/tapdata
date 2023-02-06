package io.tapdata.storage.ftp;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.io.Serializable;
import java.util.Map;

public class FtpConfig implements Serializable {

    private String ftpHost;
    private int ftpPort = 21;
    private String ftpUsername;
    private String ftpPassword;
    private Boolean ftpSsl = false;
    private String ftpAccount;
    private String encoding;
    private Boolean ftpPassiveMode = true;
    private int ftpConnectTimeout = 60;
    private int ftpDataTimeout = 0;

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    public FtpConfig load(Map<String, Object> params) {
        assert beanUtils != null;
        beanUtils.mapToBean(params, this);
        setFtpDataTimeout(ftpDataTimeout <= 0 ? -1 : 1000 * ftpDataTimeout);
        setFtpConnectTimeout(1000 * ftpConnectTimeout);
        return this;
    }

    public String getFtpHost() {
        return ftpHost;
    }

    public void setFtpHost(String ftpHost) {
        this.ftpHost = ftpHost;
    }

    public int getFtpPort() {
        return ftpPort;
    }

    public void setFtpPort(int ftpPort) {
        this.ftpPort = ftpPort;
    }

    public String getFtpUsername() {
        return ftpUsername;
    }

    public void setFtpUsername(String ftpUsername) {
        this.ftpUsername = ftpUsername;
    }

    public String getFtpPassword() {
        return ftpPassword;
    }

    public void setFtpPassword(String ftpPassword) {
        this.ftpPassword = ftpPassword;
    }

    public Boolean getFtpSsl() {
        return ftpSsl;
    }

    public void setFtpSsl(Boolean ftpSsl) {
        this.ftpSsl = ftpSsl;
    }

    public String getFtpAccount() {
        return ftpAccount;
    }

    public void setFtpAccount(String ftpAccount) {
        this.ftpAccount = ftpAccount;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public Boolean getFtpPassiveMode() {
        return ftpPassiveMode;
    }

    public void setFtpPassiveMode(Boolean ftpPassiveMode) {
        this.ftpPassiveMode = ftpPassiveMode;
    }

    public int getFtpConnectTimeout() {
        return ftpConnectTimeout;
    }

    public void setFtpConnectTimeout(int ftpConnectTimeout) {
        this.ftpConnectTimeout = ftpConnectTimeout;
    }

    public int getFtpDataTimeout() {
        return ftpDataTimeout;
    }

    public void setFtpDataTimeout(int ftpDataTimeout) {
        this.ftpDataTimeout = ftpDataTimeout;
    }
}
