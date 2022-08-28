package com.dobybros.tccore.modules.imclient.data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.dobybros.tccore.modules.imclient.impls.data.OutgoingData;

import java.io.UnsupportedEncodingException;

public abstract class IMData {
    @JSONField(serialize = false)
    private String id;
    @JSONField(serialize = false)
    private String serverId;
    @JSONField(serialize = false)
    private String contentType;

//    public abstract byte[] toContentBytes();
//    public abstract void fromContentBytes(byte[] contentBytes);
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " id " + id + " serverId " + serverId + " contentType " + contentType + " " + contentType;
    }
    public IMData() {}
    public static IMData buildSendingData(String contentType, Class<? extends IMData> messageClass) {
        IMData message = null;
        if(messageClass != null) {
            try {
                message = messageClass.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            if(message != null) {
                message.setContentType(contentType);
            }
        }
        if(message == null ||
                message.getContentType() == null)
            throw new IllegalArgumentException("Build sending data illegal, need contentType");
        return message;
    }

    public static IMData buildReceivingData(OutgoingData outgoingData, Class<? extends IMData> messageClass) {
        IMData data = null;

        byte[] content = outgoingData.getContent();
        if(content != null) {
            try {
                String contentStr = new String(content, "utf8");
                data = JSON.parseObject(contentStr, messageClass);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        if(data != null) {
            data.setContentType(outgoingData.getContentType());
            data.setId(outgoingData.getId());
            data.setServerId(outgoingData.getId());
        }
        return data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }
}
