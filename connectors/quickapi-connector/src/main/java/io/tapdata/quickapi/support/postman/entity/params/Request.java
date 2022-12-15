package io.tapdata.quickapi.support.postman.entity.params;

import java.util.Map;

public class Request {
    String description;
    String method;
    Url url;
    Header header;
    Body body;
    public static Request create(){
        return new Request();
    }
    public static Request create(Map<String ,Object> map){
        try {
            String description;
            String method;
            Url url;
            Header header;
            Body body;
            return new Request();
        }catch (Exception e){
            return new Request();
        }
    }

    public String description(){
        return this.description;
    }
    public Request description(String description){
        this.description = description;
        return this;
    }

    public String method(){
        return this.method;
    }
    public Request method(String method){
        this.method = method;
        return this;
    }

    public Url url(){
        return this.url;
    }
    public Request id(Url url){
        this.url = url;
        return this;
    }

    public Header header(){
        return this.header;
    }
    public Request header(Header header){
        this.header = header;
        return this;
    }

    public Body body(){
        return this.body;
    }
    public Request id(Body body){
        this.body = body;
        return this;
    }
}
