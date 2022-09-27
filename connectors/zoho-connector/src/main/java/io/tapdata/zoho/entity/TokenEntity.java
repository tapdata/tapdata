package io.tapdata.zoho.entity;

import io.tapdata.zoho.enums.HttpCode;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoString;

import java.util.HashMap;
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
            Map<String, Object> postResult = result.getResult();
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

            Object errorObj = postResult.get("error");
            if (errorObj instanceof String){
                HttpCode httpCode = HttpCode.code((String)errorObj);
                return this.message(Checker.isEmpty(httpCode)?"Error.":httpCode.getMessage())
                        .code(Checker.isEmpty(httpCode)?"ERROR":httpCode.getCode());
            }else {
                return this.message("Error.").code("ERROR");
            }
        }
        Map<String,Object> resultMap = result.getResult();
        Object accessTokenObj =  resultMap.get("access_token");
        Object refreshTokenObj = resultMap.get("refresh_token");
        Object tokenTypeObj =    resultMap.get("token_type");
        Object expiresInObj =    resultMap.get("expires_in");
        return this.accessToken(Checker.isEmpty(accessTokenObj)?"":(String)accessTokenObj)
                   .refreshToken(Checker.isEmpty(refreshTokenObj)?"":(String)refreshTokenObj)
                   .tokenType(Checker.isEmpty(tokenTypeObj)?"":(String)tokenTypeObj)
                   .expiresIn(Checker.isEmpty(expiresInObj)?0:(Integer) expiresInObj)
                   .message(HttpCode.SUCCEED.getMessage())
                    .code(HttpCode.SUCCEED.getCode());
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