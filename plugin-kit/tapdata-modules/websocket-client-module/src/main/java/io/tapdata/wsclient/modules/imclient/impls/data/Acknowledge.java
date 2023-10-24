package io.tapdata.wsclient.modules.imclient.impls.data;

import io.tapdata.modules.api.net.data.Data;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

public class Acknowledge extends Data {
    public static byte TYPE = 60;
    private String id;
    private Set<String> msgIds;
    private String service;

    public Acknowledge(){
        super(TYPE);
    }

    public String toString(){
        StringBuffer buffer = new StringBuffer();
        buffer.append(Arrays.toString(msgIds.toArray()));
        return new String(buffer);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Set<String> getMsgIds() {
        return msgIds;
    }

    public void setMsgIds(Set<String> msgIds) {
        this.msgIds = msgIds;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

}
