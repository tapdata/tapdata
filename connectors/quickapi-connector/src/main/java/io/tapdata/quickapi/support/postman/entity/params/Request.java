package io.tapdata.quickapi.support.postman.entity.params;

public class Request {
    String description;
    String method;
    Url url;
    Header header;
    Body body;

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
