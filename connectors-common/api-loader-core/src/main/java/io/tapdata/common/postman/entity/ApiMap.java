package io.tapdata.common.postman.entity;

import io.tapdata.common.postman.entity.params.*;
import io.tapdata.common.postman.enums.PostParam;

import java.util.*;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.base.ConnectorBase.toJson;


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
            ApiEntity apiEntity = this.stream().filter(entity -> {
                if (null == entity) return false;
                String name = entity.name();
                String url = entity.url();
                return Objects.equals(urlOrName, name) || (Objects.equals(urlOrName, url) && Objects.equals(method, entity.api().request().method()));
            }).findFirst().get();
            return apiEntity.copyOne();
        }catch (Exception e){
            return null;
        }
    }



    public static class ApiEntity{
        String url;
        String name;
        String method;
        Api api;
        String apiJson;
        public static ApiEntity create(){
            return new ApiEntity();
        }
        public static ApiEntity create(String url,String name,Api api){
            return new ApiEntity(url,name,api);
        }
        public static ApiEntity create(String url,String name,String apiJson){
            return new ApiEntity(url,name,apiJson);
        }
        public ApiEntity(String url,String name,Api api){
            this.url = url;
            this.api = api;
            this.name = name;
        }
        public ApiEntity(String url,String name,String apiJson){
            this.url = url;
            this.apiJson = apiJson;
            this.name = name;
        }
        public ApiEntity(){

        }
        public String url(){
            return this.url;
        }
        public String apiJson(){
            return this.apiJson;
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
        public ApiEntity apiJson(String apiJson){
            this.apiJson = apiJson;
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

        public Api variableAssignment(Map<String, Object> params){
            Api baseApi = null;
            Request baseRequest = null;
            List<Header> baseHeard = null;
            Body<?> baseBody = null;
            Request request = Request.create();
            Url url = null;
            if (Objects.isNull(this.api)){
                baseApi = generateApiEntity((Map<String, Object>) fromJson(this.apiJson));
                //final String[] json = {this.apiJson};
                //if (Objects.nonNull(params) && !params.isEmpty() && Objects.nonNull(json[0])){
                //    params.forEach((key,value)-> json[0] = json[0].replaceAll(key,String.valueOf(value)));
                //}
                //return generateApiEntity(JSONUtil.parseObj(json[0]));
            }else {
                baseApi = this.api;
            }
            baseRequest = baseApi.request();
            url = baseApi.request().url();
            baseHeard = baseApi.request().header();
            baseBody = baseApi.request().body();

            //对url中接口参数和查询参数进行赋值
            Url assignmentUrl = url.variableAssignment(params);


            List<Header> headers = new ArrayList<>();
            if (Objects.nonNull(baseHeard) && !baseHeard.isEmpty()){
                baseHeard.stream().filter(Objects::nonNull).forEach(heard-> headers.add(heard.variableAssignment(params)));
            }

            Body<?> body = baseBody.variableAssignment(params);
            request.url(assignmentUrl)
                    .description(baseRequest.description())
                    .method(baseRequest.method())
                    .header(headers)
                    .body(body);
            return Api.create().request(request).id(baseApi.id()).nameFullDetail(baseApi.name());
        }

        public Api generateApiEntity(){
            return generateApiEntity((Map<String, Object>) fromJson(this.apiJson));
        }

        public static Api generateApiEntity(Map<String,Object> apiMap){
            try {
                String id = (String) apiMap.get(PostParam.ID);
                String name = (String) apiMap.get(PostParam.NAME);
                Request request = Request.create((Map<String, Object>) apiMap.get(PostParam.REQUEST));
                String response = toJson(apiMap.get(PostParam.RESPONSE));
                return Api.create().id(id).nameFullDetail(name).request(request).response(response);
            }catch (Exception e){
                return Api.create();
            }
        }

        public ApiEntity copyOne(){
           return ApiEntity.create()
                   .url(this.url)
                   .method(this.method)
                   .name(this.name)
                   .api(this.api.copyOne())
                   .apiJson(this.apiJson);
        }
    }
}
