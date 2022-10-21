package io.tapdata.zoho.entity;

import io.tapdata.zoho.service.zoho.loader.ZoHoBase;

import java.io.Serializable;
import java.util.Map;

public class RefreshTokenEntity extends HttpBaseEntity implements Serializable {
    private final static long serialVersionID = 1L;
    /**
    {
        "access_token": "1000.4c15607cdb92a91c3acc96e19c400021.cb2dd3413de77e2bf0e88b261e8ae6be",
            "api_domain": "https://www.zohoapis.com.cn",
            "token_type": "Bearer",
            "expires_in": 3600
    }**/
    protected String accessToken;
    protected String apiDomain;
    protected String tokenType;
    protected int expiresIn;
    public RefreshTokenEntity(){}
    public String accessToken(){
        return this.accessToken;
    }
    public String apiDomain(){
        return this.apiDomain;
    }
    public String tokenType(){
        return this.tokenType;
    }
    public int expiresIn(){
        return this.expiresIn;
    }
    public RefreshTokenEntity(String accessToken, String apiDomain, String tokenType, int expiresIn) {
        this.accessToken = accessToken;
        this.apiDomain = apiDomain;
        this.tokenType = tokenType;
        this.expiresIn = expiresIn;
    }
    public RefreshTokenEntity entity(HttpResult result){

        return this;
    }
    public RefreshTokenEntity(Map<String,Object> map) {
        if (null != map && !map.isEmpty()){
             Object accessTokenObj= map.get("access_token");
             Object apiDomainObj = map.get("api_Domain");
             Object tokenTypeObj = map.get("token_type");
             Object expiresInObj = map.get("expires_in");
            this.accessToken = null == accessTokenObj?null:String.valueOf(accessTokenObj);
            if (null != this.accessToken && !"".equals(this.accessToken.trim())){
                if (!this.accessToken.startsWith(ZoHoBase.ZO_HO_ACCESS_TOKEN_PREFIX)){
                    this.accessToken = ZoHoBase.ZO_HO_ACCESS_TOKEN_PREFIX + this.accessToken;
                }
            }
            this.apiDomain = null == apiDomainObj?null:String.valueOf(apiDomainObj);
            this.tokenType = null == tokenTypeObj?null:String.valueOf(tokenTypeObj);
            this.expiresIn = (Integer)(expiresInObj);
        }
    }

    public static RefreshTokenEntity create(){return new RefreshTokenEntity();}
    public static RefreshTokenEntity create(String accessToken, String apiDomain, String tokenType, int expiresIn){
        return new RefreshTokenEntity(accessToken, apiDomain, tokenType, expiresIn);
    }
    public static RefreshTokenEntity create(Map<String,Object> map){
        return new RefreshTokenEntity(map);
    }
    public RefreshTokenEntity message(String message){
        this.message = message;
        return this;
    }
    public RefreshTokenEntity code(String code){
        this.code = code;
        return this;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getApiDomain() {
        return apiDomain;
    }

    public void setApiDomain(String apiDomain) {
        this.apiDomain = apiDomain;
    }

    public String getTokenType() {
        return tokenType;
    }

    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }

    public int getExpiresIn() {
        return expiresIn;
    }

    public void setExpiresIn(int expiresIn) {
        this.expiresIn = expiresIn;
    }
}
