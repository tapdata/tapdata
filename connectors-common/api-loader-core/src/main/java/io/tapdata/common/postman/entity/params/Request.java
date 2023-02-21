package io.tapdata.common.postman.entity.params;

import io.tapdata.common.postman.enums.PostParam;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Request {
    String description;
    String method;
    Url url;
    List<Header> header;
    Body<?> body;
    public Request copyOne(){
        Request request = new Request();
        request.description(description);
        request.method(method);
        request.url(url.copyOne());
        request.header(header);
        request.body(body.copyOne());
        return request;
    }
    public static Request create(){
        return new Request();
    }
    public static Request create(Map<String ,Object> map){
        try {
            Object descriptionObj = map.get(PostParam.DESCRIPTION);
            String description = Objects.nonNull(descriptionObj) ? (String)descriptionObj : null;
            Object methodObj = map.get(PostParam.METHOD);
            String method = Objects.nonNull(methodObj) ? (String) map.get(PostParam.METHOD) : null;
            Url url = Url.create(map.get(PostParam.URL));
            Object headObj = map.get(PostParam.HEADER);
            List<Header> header = Objects.isNull(headObj)? null: Header.create((List<Map<String, Object>>) headObj);
            Object bodyObj = map.get(PostParam.BODY);
            Body<?> body = Objects.isNull(bodyObj)?Body.createNoOne():Body.create((Map<String, Object>) bodyObj);
            return new Request()
                    .header(header)
                    .method(method)
                    .description(description)
                    .url(url)
                    .body(body);
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
    public Request url(Url url){
        this.url = url;
        return this;
    }

    public List<Header> header(){
        return this.header;
    }
    public Request header(List<Header> header){
        this.header = header;
        return this;
    }

    public Body body(){
        return this.body;
    }
    public Request body(Body body){
        this.body = body;
        return this;
    }
}
