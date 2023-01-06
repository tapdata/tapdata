package io.tapdata.storege.oss;

import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * Author:Skeet
 * Date: 2023/1/3
 **/
public class OssConfig implements Serializable {
    private String accessKeyId;
    private String accessKeySecret;
    private String bucketName;
    private String endpoint;

    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util

    public OssConfig load(Map<String, Object> params) {
        assert beanUtils != null;
        beanUtils.mapToBean(params, this);
        return this;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
}
