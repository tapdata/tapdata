package io.tapdata.zoho.entity;

import io.tapdata.zoho.enums.HttpCode;
import lombok.Data;

import java.util.Map;

@Data
public class HttpResult {
    String code;
    Map<String,Object> result;
    public static HttpResult create(String code,Map<String,Object> result){
        return new HttpResult(code, result);
    }
    public static HttpResult create(HttpCode code, Map<String,Object> result){
        return new HttpResult(code.getCode(), result);
    }
    private HttpResult(String code,Map<String,Object> result){
        this.code = code;
        this.result = result;
    }
    public boolean isInvalidOauth(){
        return HttpCode.INVALID_OAUTH.getCode().equals(this.code);
    }
}
