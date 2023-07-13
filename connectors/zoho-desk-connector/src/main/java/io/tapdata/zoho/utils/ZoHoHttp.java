package io.tapdata.zoho.utils;

import cn.hutool.http.*;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.enums.HttpCode;

import java.util.Collections;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.base.ConnectorBase.toJson;

/***
 * 您可在两分钟内调用ZoHo API 100 次。如果您调用 100 次以上，则接下来 30 分钟将锁定特定 API 请求。
 *
 */
public class ZoHoHttp {
    private static final String TAG = ZoHoHttp.class.getSimpleName();
    private HttpEntity<String,String> heard;
    private HttpEntity<String,Object> body;
    private HttpEntity<String,Object> form;
    private HttpType httpType;
    private String url;
    private HttpEntity<String,String> resetFull;
    private String refreshToken;
    private final HttpResult EMPTY = HttpResult.create(HttpCode.ERROR,HttpEntity.create().build(HttpCode.ERROR.getCode(), HttpCode.EMPTY.getMessage()).entity());

    private ZoHoHttp(){}
    public static ZoHoHttp create(HttpType httpType){
        return new ZoHoHttp().httpType(httpType);
    }
    public static ZoHoHttp create(String url,HttpType httpType){
        return new ZoHoHttp().url(url).httpType(httpType);
    }
    public static ZoHoHttp create(String url,HttpType httpType,HttpEntity<String,String> heard){
        return new ZoHoHttp().url(url).httpType(httpType).header(heard);
    }
    public static ZoHoHttp create(String url,HttpType httpType,HttpEntity<String,String> heard,HttpEntity<String,Object> body){
        return new ZoHoHttp().url(url).httpType(httpType).header(heard).body(body);
    }
    public ZoHoHttp body(String refreshToken){
        this.refreshToken = refreshToken;
        return this;
    }
    public ZoHoHttp body(HttpEntity<String,Object> body){
        this.body = body;
        return this;
    }
    public ZoHoHttp header(HttpEntity<String,String> header){
        this.heard = header;
        return this;
    }
    public ZoHoHttp url(String url){
        this.url = url;
        return this;
    }
    public ZoHoHttp httpType(HttpType httpType){
        this.httpType = httpType;
        return this;
    }
    public ZoHoHttp httpType(String httpType){
        this.httpType = HttpType.set(httpType);
        return this;
    }
    public ZoHoHttp resetFull(HttpEntity<String,String> resetFull){
        this.resetFull = resetFull;
        return this;
    }
    public ZoHoHttp form(HttpEntity<String,Object> form){
        this.form = form;
        return this;
    }
    private void beforeSend(){
        if (Checker.isEmpty(this.refreshToken)){
//            TapLogger.debug(TAG,"refresh_token is empty.");
        }
        if (Checker.isEmpty(this.httpType)){
            throw new RuntimeException("HTTP Method is not define Type :[POST | GET]");
        }
        if (Checker.isEmpty(this.url)){
            throw new RuntimeException("HTTP URL must be not null or not be empty.");
        }
        if (Checker.isNotEmpty(this.resetFull)){
            for (Map.Entry<String,String> entity : this.resetFull.entity().entrySet()){
                this.url = this.url.replaceAll("\\{"+entity.getKey()+"}",entity.getValue());
            }
        }
    }

    /**
     *  Please do not use null as the return value of this method
     * */
    private HttpResult afterSend(HttpResponse execute){
        if (Checker.isEmpty(execute) ){
            return EMPTY;
        }
        String body = execute.body();
        if (Checker.isEmpty(body)){
            return EMPTY.httpCode(execute.getStatus());
        }
        Map<String, Object> executeObject = (Map<String, Object>) fromJson(body);
        //JSONObject executeObject = JSONUtil.parseObj(body);
        Object executeResult = executeObject.get("errorCode");
        if (Checker.isNotEmpty(executeResult) || Checker.isNotEmpty(executeResult = executeObject.get("error"))) {
            HttpCode httpCode = HttpCode.code(String.valueOf(executeResult));
            if (null == httpCode){
                return HttpResult.create(
                        HttpCode.ERROR,
                        HttpEntity.create().build(HttpCode.ERROR.getCode(), executeObject.get("message")).entity());
            }
            return HttpResult.create(
                    httpCode,
                    HttpEntity.create().build(httpCode.getCode(),httpCode.getMessage()).entity());
        }
        return HttpResult.create(HttpCode.SUCCEED,executeObject);
    }

    public HttpResult http(){
        switch (this.httpType){
            case POST:return post();
            case GET:return get();
            case PATCH:return patch();
            case DELETE:return delete();
            case PUT:return put();
        }
        return post();
    }
    /**
     * @deprecated post请求，需要重试时不能直接调用此方法，请指定http类型后移步http()
     * */
    public HttpResult post(){
        this.beforeSend();
        HttpRequest request = HttpUtil.createPost(url);
        if (Checker.isNotEmpty(heard)){
            request.addHeaders(heard.entity());
        }
        if (Checker.isNotEmpty(form)){
            request.form(form.entity());
        }
        if (Checker.isNotEmpty(body)){
            request.body(toJson(body.entity()));
        }
        HttpResponse execute = null;
        try {
            execute = request.execute();
        }catch (Exception e){
//            TapLogger.info(TAG,"Http[POST] read timed out:{}",e.getMessage());
            return EMPTY;
        }

        return null == execute ?
                EMPTY:this.afterSend(execute);
    }
    /**
     * @deprecated post请求，需要重试时不能直接调用此方法，请指定http类型后移步http()
     * */
    public HttpResult get(){
        this.beforeSend();
        HttpRequest request = HttpUtil.createGet(url);
        if (Checker.isNotEmpty(heard)){
            request.addHeaders(heard.entity());
        }
        if (Checker.isNotEmpty(form)){
            request.form(form.entity());
        }
        HttpResponse execute = null;
        try {
            execute = request.execute();
        }catch (Exception e){
            TapLogger.info(TAG,"Http[Get] read timed out:{}",e.getMessage());
            return EMPTY;
        }
        return null == execute ?
                EMPTY:this.afterSend(execute);
    }
    private HttpResult delete(){
        this.beforeSend();
        HttpRequest request = HttpRequest.delete(url);
        if (Checker.isNotEmpty(heard)){
            request.addHeaders(heard.entity());
        }
        if (Checker.isNotEmpty(form)){
            request.form(form.entity());
        }
        HttpResponse execute = null;
        try {
            execute = request.execute();
        }catch (Exception e){
            TapLogger.info(TAG,"Http[Get] read timed out:{}",e.getMessage());
            return EMPTY;
        }
        return null == execute ?
                EMPTY:this.afterSend(execute);
    }
    private HttpResult patch(){
        this.beforeSend();
        HttpRequest request = HttpRequest.patch(url);
        if (Checker.isNotEmpty(heard)){
            request.addHeaders(heard.entity());
        }
        if (Checker.isNotEmpty(form)){
            request.form(form.entity());
        }
        HttpResponse execute = null;
        try {
            execute = request.execute();
        }catch (Exception e){
            TapLogger.info(TAG,"Http[Get] read timed out:{}",e.getMessage());
            return EMPTY;
        }
        return null == execute ?
                EMPTY:this.afterSend(execute);
    }
    private HttpResult put(){
        this.beforeSend();
        HttpRequest request = HttpRequest.put(url);
        if (Checker.isNotEmpty(heard)){
            request.addHeaders(heard.entity());
        }
        if (Checker.isNotEmpty(form)){
            request.form(form.entity());
        }
        HttpResponse execute = null;
        try {
            execute = request.execute();
        }catch (Exception e){
            TapLogger.info(TAG,"Http[Get] read timed out:{}",e.getMessage());
            return EMPTY;
        }
        return null == execute ?
                EMPTY:this.afterSend(execute);
    }

    public HttpEntity<String, String> getHeard() {
        return heard;
    }
    public HttpEntity<String, Object> getBody() {
        return body;
    }
    public HttpType getHttpType() {
        return httpType;
    }
    public String getUrl() {
        return url;
    }
    public HttpEntity<String, String> getResetFull() {
        return resetFull;
    }
    static class HttpAfter{
        HttpEntity<String,Object> after;
        HttpResponse execute;
        private HttpAfter( HttpResponse execute){
            this.execute = execute;
        }
        public static HttpAfter create(HttpResponse execute){
            return new HttpAfter(execute);
        }
        public Map<String,Object> after(){
            if (Checker.isEmpty(execute)){
                return Collections.emptyMap();
            }
            String body = execute.body();
            if (Checker.isEmpty(body)){
                return Collections.emptyMap();
            }
            Map<String, Object> executeObject = (Map<String, Object>) fromJson(body);
            //JSONObject executeObject = JSONUtil.parseObj(body);
            String executeResult = String.valueOf(executeObject.get("errorCode"));
            if (Checker.isNotEmpty(executeResult) && HttpCode.INVALID_OAUTH.getCode().equals(executeResult)){
                TapLogger.debug(TAG,"{},start refresh token...",HttpCode.INVALID_OAUTH.getMessage());
                return null;
            }
            return executeObject;
        }
        public HttpEntity<String,Object> result(){
            return this.after;
        }
    }
}
