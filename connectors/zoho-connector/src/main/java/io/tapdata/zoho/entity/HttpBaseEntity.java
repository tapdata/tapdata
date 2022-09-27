package io.tapdata.zoho.entity;

import lombok.Data;

@Data
public class HttpBaseEntity {
    protected String message;
    protected String code;
    public String message(){
        return this.message;
    }
    public String code(){
        return this.code;
    }
    public HttpBaseEntity message(String message){
        this.message = message;
        return this;
    }
    public HttpBaseEntity code(String code){
        this.code = code;
        return this;
    }
}
