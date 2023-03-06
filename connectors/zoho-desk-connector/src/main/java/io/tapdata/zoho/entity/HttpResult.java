package io.tapdata.zoho.entity;

import io.tapdata.zoho.enums.HttpCode;
import lombok.Data;

import java.util.Map;

public class HttpResult {
    String code;
    Object result;
    Integer httpCode;
    public static HttpResult create(String code,Object result){
        return new HttpResult(code, result);
    }
    public static HttpResult create(HttpCode code, Object result){
        return new HttpResult(code.getCode(), result);
    }
    private HttpResult(String code,Object result){
        this.code = code;
        this.result = result;
    }
    public boolean isInvalidOauth(){
        return HttpCode.INVALID_OAUTH.getCode().equals(this.code);
    }

    public Object getResult() {
        return result;
    }
    public HttpResult httpCode(Integer code){
        this.httpCode = code;
        return this;
    }
    public Integer httpCode(){
        return this.httpCode;
    }
    public void setResult(Object result) {
        this.result = result;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
