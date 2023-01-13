package io.tapdata.common.support.entitys;

import java.util.Map;

public class APIResponse {
    private Map<String, Object> result;
    private Integer httpCode;
    private Map<String, Object> headers;
    private Map<String,Object> error;

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
    public Integer httpCode(){
        return this.httpCode;
    }
    public APIResponse httpCode(Integer httpCode){
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
    public Map<String,Object> error(){
        return this.error;
    }
    public APIResponse error(Map<String, Object> error){
        this.error = error;
        return this;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }

    public Integer getHttpCode() {
        return httpCode;
    }

    public void setHttpCode(Integer httpCode) {
        this.httpCode = httpCode;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    public Map<String, Object> getError() {
        return error;
    }

    public void setError(Map<String, Object> error) {
        this.error = error;
    }
}
