package io.tapdata.storage.smb;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.io.Serializable;
import java.util.Map;

public class SmbConfig implements Serializable {

    private String smbHost;
    private String smbUsername;
    private String smbPassword;
    private String smbDomain;
    private String smbShareDir;

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    public SmbConfig load(Map<String, Object> params) {
        assert beanUtils != null;
        beanUtils.mapToBean(params, this);
        return this;
    }

    public String getSmbHost() {
        return smbHost;
    }

    public void setSmbHost(String smbHost) {
        this.smbHost = smbHost;
    }

    public String getSmbUsername() {
        return smbUsername;
    }

    public void setSmbUsername(String smbUsername) {
        this.smbUsername = smbUsername;
    }

    public String getSmbPassword() {
        return smbPassword;
    }

    public void setSmbPassword(String smbPassword) {
        this.smbPassword = smbPassword;
    }

    public String getSmbDomain() {
        return smbDomain;
    }

    public void setSmbDomain(String smbDomain) {
        this.smbDomain = smbDomain;
    }

    public String getSmbShareDir() {
        return smbShareDir;
    }

    public void setSmbShareDir(String smbShareDir) {
        this.smbShareDir = smbShareDir;
    }
}
