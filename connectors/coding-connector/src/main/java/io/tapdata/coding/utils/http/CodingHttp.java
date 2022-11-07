package io.tapdata.coding.utils.http;

import cn.hutool.http.*;
import cn.hutool.json.JSONUtil;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.logger.TapLogger;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CodingHttp {
    private static final String TAG = CodingHttp.class.getSimpleName();
    private Map<String,Object> body;
    private Map<String,String> heads;
    private String url;
    private Boolean keepAlive;

    public static CodingHttp create(Map<String,String> heads,Map<String,Object> body,String url){
        return new CodingHttp(heads, body, url);
    }
    public static CodingHttp create(Map<String,String> heads,String url){
        return new CodingHttp(heads,null, url);
    }
    private CodingHttp(Map<String,String> heads,Map<String,Object> body,String url){
        this.body = body;
        this.heads = heads;
        this.url = url;
    }
    public CodingHttp keepAlive(){
        this.keepAlive = Boolean.TRUE;
        return this;
    }
    public CodingHttp keepNotAlive(){
        this.keepAlive = Boolean.FALSE;
        return this;
    }
    public CodingHttp body(Map<String,Object> body){
        this.body = body;
        return this;
    }
    public HttpRequest createHttpRequest(){
        return HttpUtil.createPost(url)
                .addHeaders(this.heads.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> (String)e.getValue()))
        );
    }
    public Map<String,Object> post(){
        HttpRequest request = HttpUtil.createPost(url);
        if (null != heads) {
            request.addHeaders(this.heads.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> (String)e.getValue()))
            );
        }
        return this.post(request);
    }
    public Map<String,Object> postWithError(){
        HttpRequest request = HttpUtil.createPost(url);
        if (null != heads) {
            request.addHeaders(this.heads.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> (String)e.getValue()))
            );
        }
        return this.postWithError(request);
    }

    /**
     * 这个post需要传参，能保持多次调用同一请求时避免创建重复的HttpRequest
     * @param request
     * @return
     */
    public Map<String,Object> post(HttpRequest request){
        Map<String,Object> result = postWithError(request);
        if(result == null)
            throw new RuntimeException("Parse response empty, url: "+request.getUrl());
        Map<String,Object> response = (Map<String, Object>) result.get("Response");
        if(response == null)
            throw new RuntimeException("Parse response empty, url: "+ request.getUrl());
        Object error = result.get("Error");
        if (null != error){
            String errorMessage = String.valueOf(((Map<String,Object>)error).get("Message"));
            //String code = String.valueOf(((Map<String,Object>)error).get("Code"));
            return new HashMap<String,Object>(){{put(errorKey,"Coding request error - message: "+errorMessage);}};
        }
        error = response.get("Error");
        if (null != error){
            String errorMessage = String.valueOf(((Map<String,Object>)error).get("Message"));
            //String code = String.valueOf(((Map<String,Object>)error).get("Code"));
            throw new RuntimeException("Coding request error - message: " + errorMessage);
        }
        return result;
    }

    public Map<String,Object> postWithError(HttpRequest request){
        if (null == request){
            if (null != heads) {
                request = HttpUtil.createPost(url).addHeaders(this.heads.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()))
                );
            }
        }
        if (null != body){
            request.body(JSONUtil.toJsonStr(body), ContentType.JSON.getValue());
        }
        HttpResponse execute = null;
        try {
            execute = request.execute();
        }catch (Exception e){
            TapLogger.info(TAG,"Read timed out:{}",e.getMessage());
//            return Collections.emptyMap();
            throw new RuntimeException("execute request " + request.getUrl() + " failed, " + e.getMessage(), e);
        }
        if (null == execute){
            TapLogger.info(TAG,"Coding request error HttpResponse is null.");
            throw new RuntimeException("Coding request error HttpResponse is null. url: "+request.getUrl());
//            return Collections.emptyMap();
        }
        if ( execute.getStatus() != HttpStatus.HTTP_OK){
            TapLogger.info(TAG,"Coding request error http status:{}",execute.getStatus());
            throw new RuntimeException("Coding request error http status:"+execute.getStatus()+", url "+request.getUrl());
//            return Collections.emptyMap();
        }
        String body = execute.body();
        if (null == body || "".equals(body)){
            TapLogger.info(TAG,"Coding request error HttpResponse body is null or empty");
//            return Collections.emptyMap();
            throw new RuntimeException("Coding request return empty body, url: "+request.getUrl());
        }
        Map<String,Object> result = JSONUtil.parseObj(execute.body());
        return result;
    }

    private final String errorKey = "ERROR";
    public String errorMsg(Map<String,Object> responseMap){
        Object error = responseMap.get("Error");
        if (Checker.isNotEmpty(error)) return String.valueOf(error);
        return String.valueOf(responseMap.get(errorKey));
    }

    public CodingHttp buildBody(String key,Object value){
        if (null != this.body) {
            this.body.put(key, value);
        }
        return this;
    }
    public CodingHttp buildHeard(String key,String value){
        if (null == this.heads){
            this.heads.put(key,value);
        }
        return this;
    }
    public CodingHttp buildBodyIfAbsent(String key,Object value){
        if (null != value && !( value instanceof Map && ((Map)value).isEmpty() )){
            this.buildBody(key, value);
        }
        return this;
    }
    public CodingHttp buildHeardIfAbsent(String key,String value){
        if (null != value){
            this.buildBody(key, value);
        }
        return this;
    }
}
