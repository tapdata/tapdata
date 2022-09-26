package io.tapdata.coding.utils.http;

import cn.hutool.http.*;
import cn.hutool.json.JSONUtil;
import io.tapdata.entity.logger.TapLogger;

import java.util.Collections;
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

    /**
     * 这个post需要传参，能保持多次调用同一请求时避免创建重复的HttpRequest
     * @param request
     * @return
     */
    private int retry = 0;
    public Map<String,Object> post(HttpRequest request){
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
            Thread.sleep(10);//防止刷爆别人的接口，设置阻塞
            execute = request.execute();
        }catch (Exception e){
            TapLogger.info(TAG,"Read timed out:{}",e.getMessage());
            if (retry++>3){
                return Collections.emptyMap();
            }
            return this.post(request);
        }
        retry = 0;
        return null == execute || execute.getStatus() != HttpStatus.HTTP_OK ?
                Collections.emptyMap() : JSONUtil.parseObj(execute.body());
    }
}
