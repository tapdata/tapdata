package io.tapdata.coding.utils.http;

import cn.hutool.http.*;
import cn.hutool.json.JSONUtil;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.logger.TapLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class CodingHttp {
    private static final String TAG = CodingHttp.class.getSimpleName();
    private Map<String, Object> body;
    private Map<String, String> heads;
    private String url;
    private Boolean keepAlive;

    public static CodingHttp create(Map<String, String> heads, Map<String, Object> body, String url) {
        return new CodingHttp(heads, body, url);
    }

    public static CodingHttp create(Map<String, String> heads, String url) {
        return new CodingHttp(heads, null, url);
    }

    private CodingHttp(Map<String, String> heads, Map<String, Object> body, String url) {
        this.body = body;
        this.heads = heads;
        this.url = url;
    }

    public CodingHttp keepAlive() {
        this.keepAlive = Boolean.TRUE;
        return this;
    }

    public CodingHttp keepNotAlive() {
        this.keepAlive = Boolean.FALSE;
        return this;
    }

    public CodingHttp body(Map<String, Object> body) {
        this.body = body;
        return this;
    }

    public HttpRequest createHttpRequest() {
        return HttpUtil.createPost(url)
                .addHeaders(this.heads.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()))
                );
    }

    public Map<String, Object> post() {
        HttpRequest request = HttpUtil.createPost(url);
        if (null != heads) {
            request.addHeaders(this.heads.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()))
            );
        }
        return this.post(request);
    }

    public Map<String, Object> postWithError() {
        HttpRequest request = HttpUtil.createPost(url);
        if (null != heads) {
            request.addHeaders(this.heads.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()))
            );
        }
        return this.postWithError(request);
    }

    /**
     * 这个post需要传参，能保持多次调用同一请求时避免创建重复的HttpRequest
     *
     * @param request
     * @return
     */
    public Map<String, Object> post(HttpRequest request) {
        Map<String, Object> result = postWithError(request);
        if (Objects.isNull(result)) {
            HashMap<String, Object> errorMap = new HashMap<>();
            errorMap.put(this.errorKey, String.format("Cannot get the http response result, request url - %s, request body - %s", request.getUrl(), toJson(this.body)));
            return errorMap;
        }
        Object error = result.get("Error");
        if (Objects.nonNull(error)) {
            String errorMessage = String.valueOf(((Map<String, Object>) error).get("Message"));
            HashMap<String, Object> errorMap = new HashMap<>();
            errorMap.put(this.errorKey, String.format("Coding request error - response message: %s, request url:%s, request body: %s.", errorMessage, request.getUrl(), toJson(this.body)));
            return errorMap;
        }
        Object responseObj = result.get("Response");
        if (Objects.isNull(responseObj)) {
            TapLogger.warn(TAG, String.format("Cannot get the param which name is 'Response' from http response body, request url - %s, request body - %s", request.getUrl(), toJson(this.body)));
            return result;
        }
        Map<String, Object> response = (Map<String, Object>) responseObj;
        if (Objects.nonNull(error = response.get("Error"))) {
            String errorMessage = String.valueOf(((Map<String, Object>) error).get("Message"));
            HashMap<String, Object> errorMap = new HashMap<>();
            errorMap.put(this.errorKey, String.format("Coding request error - response message: %s, response body:%s , request url:%s, request body: %s.", errorMessage, toJson(response), request.getUrl(), toJson(this.body)));
            return errorMap;
        }
        return result;
    }

    public Map<String, Object> postWithError(HttpRequest request) {
        if (null == request) {
            if (Objects.nonNull(this.heads)) {
                request = HttpUtil.createPost(this.url).addHeaders(this.heads.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()))
                );
            }
        }
        if (Objects.nonNull(this.body)) {
            request.body(JSONUtil.toJsonStr(this.body), ContentType.JSON.getValue());
        }
        HttpResponse execute = null;
        try {
            execute = request.execute();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Coding request failed - error message: %s, request url:%s, request body: %s.", e.getMessage(), request.getUrl(), toJson(this.body)));
        }
        if (null == execute) {
            throw new RuntimeException(String.format("Coding request failed, HttpResponse is empty. request url:%s, request body: %s.", request.getUrl(), toJson(this.body)));
        }
        if (execute.getStatus() != HttpStatus.HTTP_OK) {
            throw new RuntimeException(String.format("Coding request failed with http code %s. request url:%s, request body: %s.", execute.getStatus(), request.getUrl(), toJson(this.body)));
        }
        String body = execute.body();
        if (null == body || "".equals(body)) {
            throw new RuntimeException(String.format("Coding request failed with empty response body. request url:%s, request body: %s.", request.getUrl(), toJson(this.body)));
        }
        return JSONUtil.parseObj(execute.body());
    }

    private final String errorKey = "ERROR";

    public String errorMsg(Map<String, Object> responseMap) {
        Object error = responseMap.get("Error");
        if (Checker.isNotEmpty(error)) return String.valueOf(error);
        return String.valueOf(responseMap.get(errorKey));
    }

    public CodingHttp buildBody(String key, Object value) {
        if (null != this.body) {
            this.body.put(key, value);
        }
        return this;
    }

    public CodingHttp buildHeard(String key, String value) {
        if (null == this.heads) {
            this.heads.put(key, value);
        }
        return this;
    }

    public CodingHttp buildBodyIfAbsent(String key, Object value) {
        if (null != value && !(value instanceof Map && ((Map) value).isEmpty())) {
            this.buildBody(key, value);
        }
        return this;
    }

    public CodingHttp buildHeardIfAbsent(String key, String value) {
        if (null != value) {
            this.buildBody(key, value);
        }
        return this;
    }
}
