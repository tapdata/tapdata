package io.tapdata.modules.api.net.entity;

import io.tapdata.entity.serializer.JavaCustomSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author GavinXiao
 * @description SubscribeURLToken create by Gavin
 * @create 2023/6/14 15:28
 **/
public class SubscribeURLToken extends SubscribeToken implements JavaCustomSerializer {
    private String userId;
    private Long expireSeconds;

    @Override
    public void from(InputStream inputStream) throws IOException {
        super.from(inputStream);
        DataInputStream dis = new DataInputStream(inputStream);
        randomId = dis.readUTF();
        userId = dis.readUTF();
        expireSeconds = dis.readLong();
    }

    @Override
    public void to(OutputStream outputStream) throws IOException {
        super.to(outputStream);
        DataOutputStream dos = new DataOutputStream(outputStream);
        dos.writeUTF(randomId);
        dos.writeUTF(userId);
        if (null != expireSeconds) {
            dos.writeLong(expireSeconds);
        }
    }

    public Long getExpireSeconds() {
        return expireSeconds;
    }

    public void setExpireSeconds(Long expireSeconds) {
        this.expireSeconds = expireSeconds;
    }

    public String getSupplierKey() {
        return supplierKey;
    }

    public void setSupplierKey(String supplierKey) {
        this.supplierKey = supplierKey;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
