package io.tapdata.zoho.entity;

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
}
