package io.tapdata.common.support.entitys;

import java.io.Serializable;
import java.util.Map;

public class APIResponse implements Serializable {
    public static final long serialversionUID = 1L;
    public Map<String, Object> result;
    public int httpCode;
    public Map<String, Object> headers;

    public static APIResponse create(){
        return new APIResponse();
    }
    public static APIResponse create(Integer httpCode, Map<String, String> headers, Map<String, Object> result){
        return new APIResponse();
    }
    public APIResponse(){

    }
    public APIResponse(Integer httpCode,Map<String, Object> headers,Map<String, Object> result){
        this.result = result;
        this.headers = headers;
        this.httpCode = httpCode;
    }
    public Map<String,Object> result(){
        return this.result;
    }
    public APIResponse result(Map<String, Object> result){
        this.result = result;
        return this;
    }
    public int httpCode(){
        return this.httpCode;
    }
    public APIResponse httpCode(int httpCode){
        this.httpCode = httpCode;
        return this;
    }
    public Map<String,Object> headers(){
        return this.headers;
    }
    public APIResponse headers(Map<String, Object> headers){
        this.headers = headers;
        return this;
    }
    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }

    public int getHttpCode() {
        return httpCode;
    }

    public void setHttpCode(int httpCode) {
        this.httpCode = httpCode;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

}
