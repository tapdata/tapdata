package io.tapdata.zoho.entity;

import io.tapdata.entity.error.CoreException;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;

public class ContextConfig {
    public static ContextConfig create(){
        return new ContextConfig();
    }
    private String refreshToken;
    private String accessToken;
    private String clientId;
    private String clientSecret;
    private String orgId;
    private String generateCode;
    private String connectionMode;
    private String streamReadType;
    private boolean sortType;
    private String fields;
    private boolean needDetailObj;

    public ContextConfig refreshToken(String refreshToken){
        this.refreshToken = refreshToken;
        return this;
    }
    public ContextConfig streamReadType(String streamReadType){
        this.streamReadType = streamReadType;
        return this;
    }

    public ContextConfig connectionMode(String connectionMode){
        this.connectionMode = connectionMode;
        return this;
    }
    public ContextConfig accessToken(String accessToken){
        this.accessToken = accessToken;
        return this;
    }
    public ContextConfig clientId(String clientId){
        this.clientId = clientId;
        return this;
    }
    public ContextConfig clientSecret(String clientSecret){
        this.clientSecret = clientSecret;
        return this;
    }
    public ContextConfig orgId(String orgId){
        this.orgId = orgId;
        return this;
    }
    public ContextConfig generateCode(String generateCode){
        this.generateCode = generateCode;
        return this;
    }
    public ContextConfig fields(Object fieldObj){
        //List<Map<String,Object>>
        if (fieldObj instanceof Collection) {
            Collection<Object> objects = (Collection<Object>) fieldObj;
            StringJoiner joiner = new StringJoiner(",");
            AtomicInteger num = new AtomicInteger();
            for (Object f : objects) {
                if (null == f) continue;
                if (f instanceof Map){
                    Map<String, Object> map = (Map<String, Object>) f;
                    Optional.ofNullable(map.get("keyName")).ifPresent(name -> {
                        String n = String.valueOf(name);
                        if (!"".equals(n.trim()) && num.get() < 30){//joiner.length() <= (100-n.length())){
                            joiner.add(n);
                            num.getAndIncrement();
                        } else {
                            throw new CoreException("".equals(n.trim()) ?
                                    "An error custom field name, field name can not be empty"
                                    : "An error custom field name, the cumulative count of all field names cannot exceed 30");
                        }
                    });
                }
            }
            fields = joiner.toString();
        } else {
            fields = null;
        }
        return this;
    }
    public ContextConfig sortType(Object sortObj){
        this.sortType = true;
        try {
            if (sortObj instanceof Boolean) {
                this.sortType = (Boolean)sortObj;
            } else if (sortObj instanceof String){
                this.sortType = Boolean.getBoolean((String) sortObj);
            }
        }catch (Exception ignore) {}
        return this;
    }
    public ContextConfig needDetailObj(Object needDetailObj){
        this.needDetailObj = true;
        try {
            if (needDetailObj instanceof Boolean) {
                this.needDetailObj = (Boolean)needDetailObj;
            } else if (needDetailObj instanceof String){
                this.needDetailObj = Boolean.getBoolean((String) needDetailObj);
            }
        }catch (Exception ignore) {}
        return this;
    }

    public String refreshToken(){
        return this.refreshToken;
    }
    public String streamReadType(){
        return this.streamReadType;
    }

    public String connectionMode(){
        return this.connectionMode;
    }
    public String accessToken(){
        return this.accessToken;
    }
    public String clientId(){
        return this.clientId;
    }
    public String clientSecret(){
        return this.clientSecret;
    }
    public String orgId(){
        return this.orgId;
    }
    public String generateCode(){
        return this.generateCode;
    }

    public boolean sortType(){
        return this.sortType;
    }
    public boolean needDetailObj(){
        return this.needDetailObj;
    }
    public String fields(){
        return this.fields;
    }
}
