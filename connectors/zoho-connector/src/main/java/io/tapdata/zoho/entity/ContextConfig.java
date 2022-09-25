package io.tapdata.zoho.entity;

//import io.tapdata.coding.enums.IssueType;

import lombok.Data;

@Data
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


    private String projectName;
    private String token;
    private String teamName;
    private String iterationCodes;
    private String connectionMode;
    private String streamReadType;

    public ContextConfig projectName(String projectName){
        this.projectName = projectName;
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
    public ContextConfig token(String token){
        this.token = token;
        return this;
    }
    public ContextConfig teamName(String teamName){
        this.teamName = teamName;
        return this;
    }
    public ContextConfig iterationCodes(String iterationCodes){
        this.iterationCodes = iterationCodes;
        return this;
    }
}
