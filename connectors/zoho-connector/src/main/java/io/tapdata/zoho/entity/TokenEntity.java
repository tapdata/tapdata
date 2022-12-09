package io.tapdata.zoho.entity;

import io.tapdata.zoho.enums.HttpCode;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;

import java.util.Map;

public class TokenEntity extends HttpBaseEntity{
    protected String accessToken;
    protected String refreshToken;
    protected String tokenType;
    protected int expiresIn;

    public static TokenEntity create(){
        return new TokenEntity();
    }
    public TokenEntity entity(HttpResult result){
        if (Checker.isEmpty(result) || !HttpCode.SUCCEED.getCode().equals(result.getCode())) {
            Map<String, Object> postResult = (Map<String, Object>) result.getResult();
            //取通过Http请求过来的error属性
            /**
             * {
             *     "error": "invalid_code"
             * }
             * {
             *     "error": "invalid_client"
             * }
             * {
             *     "error": "invalid_client_secret"
             * }
             * */
            String code = result.getCode();
            HttpCode httpCode = HttpCode.code(code);
            return this.message(Checker.isEmpty(httpCode)?"ERROR":httpCode.getMessage())
                        .code(Checker.isEmpty(code)?"ERROR":httpCode.getCode());

        }
        Map<String,Object> resultMap = (Map<String, Object>) result.getResult();
        Object accessTokenObj =  resultMap.get("access_token");
        Object refreshTokenObj = resultMap.get("refresh_token");
        Object tokenTypeObj =    resultMap.get("token_type");
        Object expiresInObj =    resultMap.get("expires_in");
        return this.accessToken(Checker.isEmpty(accessTokenObj)?"": ZoHoBase.builderAccessToken((String)accessTokenObj))
                   .refreshToken(Checker.isEmpty(refreshTokenObj)?"":(String)refreshTokenObj)
                   .tokenType(Checker.isEmpty(tokenTypeObj)?"":(String)tokenTypeObj)
                   .expiresIn(Checker.isEmpty(expiresInObj)?0:(Integer) expiresInObj)
                   .message(HttpCode.SUCCEED.getMessage())
                    .code(HttpCode.SUCCEED.getCode());
    }

    public TokenEntity accessToken(String accessToken){
        this.accessToken = ZoHoBase.builderAccessToken(accessToken);
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
    public TokenEntity message(String message){
        this.message = message;
        return this;
    }
    public TokenEntity code(String code){
        this.code = code;
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
    public String message(){
            return this.message;
        }
}