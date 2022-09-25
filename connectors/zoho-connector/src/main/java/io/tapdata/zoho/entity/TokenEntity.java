package io.tapdata.zoho.entity;

import io.tapdata.zoho.enums.HttpCode;
import io.tapdata.zoho.utils.Checker;

import java.util.Map;

public class TokenEntity{
    private String accessToken;
    private String refreshToken;
    private String tokenType;
    private int expiresIn;

    public static TokenEntity create(){
        return new TokenEntity();
    }
    public TokenEntity entity(HttpResult result){
        if (Checker.isEmpty(result) || !HttpCode.SUCCEED.getCode().equals(result.getCode())) return null;
        Map<String,Object> resultMap = result.getResult();
        Object accessTokenObj = resultMap.get("access_token");
        Object refreshTokenObj = resultMap.get("refresh_token");
        Object tokenTypeObj = resultMap.get("token_type");
        Object expiresInObj = resultMap.get("expires_in");
        return this.accessToken(Checker.isEmpty(accessTokenObj)?"":(String)accessTokenObj)
                .refreshToken(Checker.isEmpty(refreshTokenObj)?"":(String)refreshTokenObj)
                .tokenType(Checker.isEmpty(tokenTypeObj)?"":(String)tokenTypeObj)
                .expiresIn(Checker.isEmpty(expiresInObj)?0:(Integer) expiresInObj);
    }
    public TokenEntity accessToken(String accessToken){
        this.accessToken = accessToken;
        return this;
    }
    public TokenEntity refreshToken(String refreshToken){
        this.refreshToken = refreshToken;
        return this;
    }
    public TokenEntity tokenType(String tokenType){
        this.tokenType = tokenType;
        return this;
    }
    public TokenEntity expiresIn(int expiresIn){
        this.expiresIn = expiresIn;
        return this;
    }
    public String accessToken(){
        return this.accessToken;
    }
    public String refreshToken(){
        return this.refreshToken ;
    }
    public String tokenType(){
        return this.tokenType;
    }
    public int expiresIn(){
            return this.expiresIn;
        }
}