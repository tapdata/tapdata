package io.tapdata.quickapi.support.postman;

import cn.hutool.json.JSONUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.api.APIInvoker;
import io.tapdata.pdk.apis.api.APIResponse;
import io.tapdata.pdk.apis.api.APIResponseInterceptor;
import io.tapdata.pdk.apis.api.comom.APIDocument;
import io.tapdata.quickapi.core.annotation.ApiType;
import io.tapdata.quickapi.support.postman.entity.ApiEvent;
import io.tapdata.quickapi.support.postman.entity.ApiInfo;
import io.tapdata.quickapi.support.postman.entity.ApiMap;
import io.tapdata.quickapi.support.postman.entity.ApiVariable;
import io.tapdata.quickapi.support.postman.entity.params.Api;
import io.tapdata.quickapi.support.postman.entity.params.Body;
import io.tapdata.quickapi.support.postman.entity.params.Header;
import io.tapdata.quickapi.support.postman.entity.params.Url;
import io.tapdata.quickapi.support.postman.enums.PostParam;
import io.tapdata.quickapi.core.emun.TapApiTag;

import io.tapdata.quickapi.support.postman.util.ReplaceTagUtil;
import okhttp3.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@ApiType
public class PostManAnalysis
        implements APIDocument<APIInvoker>, APIInvoker,APIResponseInterceptor{
    private final boolean filterUselessApi = false;//是否过滤没有被标记的api
    private final String sourcePath = "/resources/api/apiJson.json";
    private static final String TAG = PostManAnalysis.class.getSimpleName();

    PostManApiContext apiContext;
    public PostManApiContext apiContext(){
        return this.apiContext;
    }

    public static PostManAnalysis create(){
        return new PostManAnalysis();
    }
    private String packageName = "io.tapdata.coding.postman.auto";
    public static void main(String[] args) {
        PostManAnalysis analysis = new PostManAnalysis();
        analysis.builder();
    }

    public void builder(){
        Map<String,Object> json = JSONUtil.readJSONObject(new File("D:\\GavinData\\InstallPackage\\飞书\\data\\data\\Feishu_1658300789.zh_CN.postman_collection.json"), StandardCharsets.UTF_8);

        List<Object> variable = this.getList(PostParam.VARIABLE,json);

        Map<String,Object> info = this.getMap(PostParam.INFO,json);

        List<Object> list = this.getList(PostParam.ITEM,json);
        Map<String, List<Object>> listMap = item(list);

        List<Object> event = this.getList(PostParam.EVENT,json);

        String variableClass = this.variable(variable);
    }

    Map<String, Object> getMap(String key,Map<String,Object> collection){
        Object infoObj = collection.get(key);
        return (null != infoObj && infoObj instanceof Map)?((Map<String,Object>)infoObj):null;
    }
    List<Object> getList(String key, Map<String,Object> collection){
        Object infoObj = collection.get(key);
        return (null != infoObj && infoObj instanceof Collection)?(List<Object>)infoObj:null;
    }

    String variable(List<Object> variables){
        StringBuilder builder = new StringBuilder();
        builder.append("package ").append(packageName).append(";\n");
        //builder.append("").append(";\n");
        builder.append("public enum Variable{\n");

        if (null != variables && !variables.isEmpty()) {
            StringJoiner joiner = new StringJoiner(",\n");
            for (Object variable : variables) {
                if (null != variable && variable instanceof Map) {
                    Map<String,Object> var = (Map<String, Object>)variable;
                    Object key = var.get(PostParam.KEY);
                    Object value = var.get(PostParam.VALUE);
                    joiner.add(String.format("\t%s(\"%s\",\"%s\")",String.valueOf(key).toUpperCase(),key,value));
                }
            }
            builder.append(joiner.toString());
        }
        builder.append("\t;\n");
        builder.append("\tString keyName;\n");
        builder.append("\tString keyValue;\n");
        builder.append("\tVariable(String key,String value){\n");
        builder.append("\t\tthis.keyName = key;\n");
        builder.append("\t\tthis.keyValue = value;\n");
        builder.append("\t}\n");
        builder.append("\tpublic String keyValue(){\n");
        builder.append("\t\treturn this.keyValue;\n");
        builder.append("\t}\n");
        builder.append("\tpublic String keyName(){\n");
        builder.append("\t\treturn this.keyName;\n");
        builder.append("\t}\n");
        builder.append("}");
        return builder.toString();
    }

    Map<String,List<Object>> item(List<Object> item){
        if (null == item) return null;
        List<Object> list = new ArrayList<>();
        put(list,item);

        Map<String, List<Object>> listMap = list.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(obj -> {
            Map<String, Object> request = (Map<String, Object>) ((Map<String, Object>) obj).get(PostParam.REQUEST);
            Object urlObj = request.get(PostParam.URL);
            String url = "UN_KNOW_URL";
            if (null != urlObj){
                if (urlObj instanceof String){
                    url = (String) urlObj;
                }else if (urlObj instanceof Map){
                    url = String.valueOf(((Map<String,Object>) urlObj).get(PostParam.RAW));
                }
            }
            return ReplaceTagUtil.replace(url);
            //String id = String.valueOf(((Map<String, Object>) obj).get(PostParam.ID));
            //int lastIndexOf = id.lastIndexOf(".");
            //return id.substring(0, lastIndexOf);
        }));
        return listMap;
    }
    void put(List<Object> list,Object obj){
        if (null == obj) return;
        if (obj instanceof Map){
            Map<String,Object> map = (Map<String, Object>) obj;
            if (isMapFromItem(map)){
                list.add(map);
            }else{
                map.forEach( (key,value) -> put(list,value) );
            }
        }else if (obj instanceof Collection){
            Collection<Object> list1 = (Collection<Object>) obj;
            for (Object it : list1) {
                if (null == it) continue;
                put(list,it);
            }
        }
    }
    boolean isMapFromItem(Object mapObj){
        if (null != mapObj && mapObj instanceof Map){
            Map<String,Object> map = (Map<String, Object>) mapObj;
            Object request = map.get(PostParam.REQUEST);
            return null != request;
        }
        return false;
    }

    void api(List<Object> apiList,String idGroup){
        if (null == apiList || apiList.isEmpty()) return;
        StringBuilder builder = new StringBuilder();
        int lastIndexOf = idGroup.lastIndexOf(".");
        String packageName = idGroup.substring(lastIndexOf + 1);
        builder.append("package ").append(packageName).append(";\n");

        builder.append("public class ").append(packageName).append("{\n");
        for (Object api : apiList) {
            if (api instanceof Map){
                Map<String,Object> map = (Map<String, Object>) api;

            }
        }
        builder.append("}");
    }

    Api generateApiEntity(Map<String,Object> apiMap){
        try {
            String id = (String) apiMap.get(PostParam.ID);
            String name = (String) apiMap.get(PostParam.NAME);;
            io.tapdata.quickapi.support.postman.entity.params.Request request = io.tapdata.quickapi.support.postman.entity.params.Request.create();

            Map<String,Object> requestMap = (Map<String, Object>) apiMap.get(PostParam.REQUEST);
            String description = (String) requestMap.get(PostParam.DESCRIPTION);
            String method = (String) requestMap.get(PostParam.METHOD);
            Url url;
            Header header;
            Body body;

            String response;

            return null;
        }catch (Exception e){
            return null;
        }
    }

    @Override
    public APIInvoker analysis(String apiJson,String type, Map<String, Object> params) {
        Map<String, Object> json = null;
        if (Objects.nonNull(apiJson)) {
            json = JSONUtil.parseObj(apiJson);
        }else {
            try {
                json = JSONUtil.readJSONObject(new File(sourcePath),StandardCharsets.UTF_8);
            }catch (Throwable ignored){
                json = null;
            }
            if (Objects.isNull(json)){
                TapLogger.error(TAG,"Can't analysis api JSON document, please save this JSON file into " + sourcePath + ". ");
            }
        }

        List<Object> item = this.getList(PostParam.ITEM, json);
        Map<String, List<Object>> listMap = this.item(item);
        ApiMap apiMap = ApiMap.create();
        listMap.forEach((key,apiItem)->{
            apiItem.stream().filter(api->{
                if (null == api) return false;
                if (!filterUselessApi) return true;
                try {
                    Map<String,Object> apiDetail = (Map<String,Object>)api;
                    String name = (String)apiDetail.get(PostParam.NAME);
                    return TapApiTag.isLabeled(name);
                }catch (Exception e){
                    return false;
                }
            }).forEach(api->{
                try {
                    Map<String,Object> apiDetail = (Map<String,Object>)api;
                    String name = (String)apiDetail.get(PostParam.NAME);
                    Map<String,Object> request = (Map<String,Object>) apiDetail.get(PostParam.REQUEST);
                    Object urlObj = request.get(PostParam.URL);
                    String url = null;
                    if (urlObj instanceof String){
                        url = (String) urlObj;
                    }else if(urlObj instanceof Map){
                        url = (String) ((Map<String,Object>)urlObj).get(PostParam.RAW);
                    }
                    String method = (String)request.get(PostParam.METHOD);
                    apiMap.add(ApiMap.ApiEntity.create(url,name,this.generateApiEntity(apiDetail)).method(method));
                }catch (Exception ignored){ }
            });
        });


//        item.stream().filter(api->{
//            if (null == api) return false;
//            if (!filterUselessApi) return true;
//            try {
//                Map<String,Object> apiDetail = (Map<String,Object>)api;
//                String name = (String)apiDetail.get(PostParam.NAME);
//                return TapApiTag.isLabeled(name);
//            }catch (Exception e){
//                return false;
//            }
//        }).forEach(api->{
//            try {
//                Map<String,Object> apiDetail = (Map<String,Object>)api;
//                String name = (String)apiDetail.get(PostParam.NAME);
//                Map<String,Object> request = (Map<String,Object>) apiDetail.get(PostParam.REQUEST);
//                Object urlObj = request.get(PostParam.URL);
//                String url = null;
//                if (urlObj instanceof String){
//                    url = (String) urlObj;
//                }else if(urlObj instanceof Map){
//                    url = (String) ((Map<String,Object>)urlObj).get(PostParam.RAW);
//                }
//                apiMap.add(ApiMap.ApiEntity.create(name,url,this.generateApiEntity(apiDetail)));
//            }catch (Exception ignored){ }
//        });
        if (Objects.isNull(this.apiContext)){
            this.apiContext = PostManApiContext.create();
        }
        this.apiContext.info(ApiInfo.create(this.getMap(PostParam.INFO,json)))
                .event(ApiEvent.create(this.getList(PostParam.EVENT,json)))
                .variable(ApiVariable.create(this.getList(PostParam.VARIABLE,json)))
                .apis(apiMap)
                .table(
                    apiMap.stream()
                    .filter(entity -> Objects.nonNull(entity) && Objects.nonNull(entity.name()) && TapApiTag.isTableName(entity.name()))
                    .map(entity -> TapApiTag.analysisTableName(entity.name()) ).collect(Collectors.toList())
                );
        if ( Objects.nonNull(params) && !params.isEmpty() ) {
            this.apiContext.variableAdd(params);
        }
        return this;
    }


    public APIResponse http(String uriOrName, String method, Map<String, Object> params) {
        ApiMap.ApiEntity apiEntity = this.apiContext.apis().quickGet(uriOrName, method);
        if (Objects.isNull(apiEntity)) return null;

        ApiVariable variable = ApiVariable.create();
        variable.putAll(this.apiContext.variable());
        if(Objects.nonNull(params) && !params.isEmpty()) {
            variable.putAll(params);
        }

        io.tapdata.quickapi.support.postman.entity.params.Request apiRequest = apiEntity.api().request();
        String apiUrl = apiEntity.url();
        String apiMethod = apiEntity.method();
        Header<String,String> apiHeader = apiRequest.header();
        Body apiBody = apiRequest.body();

        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{\n    \"approval_code\": \"EB828003-9FFE-4B3F-AA50-2E199E2ED942\",\n    \"group_external_id\": \"1234567\",\n    \"instance_code\": \"EB828003-9FFE-4B3F-AA50-2E199E2ED943\",\n    \"instance_external_id\": \"EB828003-9FFE-4B3F-AA50-2E199E2ED976\",\n    \"locale\": \"zh-CN\",\n    \"task_start_time_from\": \"1547654251506\",\n    \"task_start_time_to\": \"1547654251506\",\n    \"task_status\": \"PENDING\",\n    \"task_title\": \"test\",\n    \"user_id\": \"lwiu098wj\"\n}\n");
        Request request = new Request.Builder()
                .url("https://open.feishu.cn/open-apis/approval/v4/tasks/search")
                .method(apiMethod, body)
                .headers(Headers.of(apiHeader))
                .addHeader("Authorization", "Bearer {{tenant_access_token}}")
                .addHeader("Content-Type", "application/json")
                .build();
        try {
            Response response = client.newCall(request).execute();
            return APIResponse.create().httpCode(response.code())
                    .result(JSONUtil.parseObj(response.body().string()))
                    .headers(JSONUtil.parseObj(response.headers().toString()));
        } catch (IOException e) {
            return  null;
        }
    }

    @Override
    public APIResponse invoke(String uriOrName, String method, Map<String, Object> params) {
        APIResponse response = this.http(uriOrName, method, params);
        response = this.intercept(response,uriOrName,method,params);
        return response;
    }

    @Override
    public APIResponse intercept(APIResponse response, String urlOrName, String method, Map<String, Object> params) {
        if(Objects.isNull(response)) {
            throw new CoreException(String.format("Http request call failed, unable to get the request result: url or name [%s], method [%s].",urlOrName,method));
        }
        APIResponse interceptorResponse = APIResponse.create();

        return interceptorResponse;
    }

    @Override
    public void setAPIResponseInterceptor(APIResponseInterceptor interceptor) {
//        this.interceptor = interceptor;
    }
}
