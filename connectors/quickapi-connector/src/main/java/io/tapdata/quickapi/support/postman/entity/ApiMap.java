package io.tapdata.quickapi.support.postman.entity;

import io.tapdata.quickapi.support.postman.entity.params.Api;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;


public class ApiMap extends HashSet<ApiMap.ApiEntity>{

    public static ApiMap create(){
        return new ApiMap();
    }

    public static ApiMap create(Collection<ApiEntity> entities){
        return new ApiMap(entities);
    }

    ApiMap(){ }

    ApiMap(Collection<ApiEntity> entities){
        this.addAll(entities);
    }

    public ApiEntity quickGet(String urlOrName, String method){
        try {
            return this.stream().filter(entity -> {
                if (null == entity) return false;
                String name = entity.name();
                String url = entity.url();
                return Objects.equals(urlOrName,name) || (Objects.equals(urlOrName,url) && Objects.equals(method,entity.api().request().method()));
            }).findFirst().get();
        }catch (Exception e){
            return null;
        }
    }



    public static class ApiEntity{
        String url;
        String name;
        String method;
        Api api;
        public static ApiEntity create(){
            return new ApiEntity();
        }
        public static ApiEntity create(String url,String name,Api api){
            return new ApiEntity(url,name,api);
        }
        public ApiEntity(String url,String name,Api api){
            this.url = url;
            this.api = api;
            this.name = name;
        }
        public ApiEntity(){

        }
        public String url(){
            return this.url;
        }
        public String method(){
            return this.method;
        }
        public String name(){
            return this.name;
        }
        public Api api(){
            return this.api;
        }
        public ApiEntity url(String url){
            this.url = url;
            return this;
        }
        public ApiEntity name(String name){
            this.name = name;
            return this;
        }
        public ApiEntity method(String method){
            this.method = method;
            return this;
        }
        public ApiEntity api(Api api){
            this.api = api;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ApiEntity apiEntity = (ApiEntity) o;
            return Objects.equals(url, apiEntity.url) && Objects.equals(name, apiEntity.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(url, name, api);
        }
    }
}
