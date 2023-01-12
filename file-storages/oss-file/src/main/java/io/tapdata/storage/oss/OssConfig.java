package io.tapdata.storage.oss;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * Author:Skeet
 * Date: 2023/1/3
 **/
public class OssConfig implements Serializable {
    private String accessKey;
    private String SecretKey;
    private String bucket;
    private String endpoint;

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    public OssConfig load(Map<String, Object> params) {
        assert beanUtils != null;
        beanUtils.mapToBean(params, this);
        return this;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return SecretKey;
    }

    public void setSecretKey(String secretKey) {
        this.SecretKey = secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
}
