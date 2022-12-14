package io.tapdata.quickapi.support.postman.entity.params;

import java.util.Objects;

public class Api {
    String id;
    String name;
    Request request;
    String response;

    public String id(){
        return this.id;
    }
    public String name(){
        return this.name;
    }
    public Request request(){
        return this.request;
    }
    public String response(){
        return this.response;
    }
    public Api id(String id){
        this.id = id;
        return this;
    }
    public Api name(String name){
        this.name = name;
        return this;
    }
    public Api request(Request request){
        this.request = request;
        return this;
    }
    public Api response(String response){
        this.response = response;
        return this;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Api api = (Api) o;
        return Objects.equals(id, api.id) && Objects.equals(name, api.name) && Objects.equals(request, api.request) && Objects.equals(response, api.response);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, request, response);
    }
}
