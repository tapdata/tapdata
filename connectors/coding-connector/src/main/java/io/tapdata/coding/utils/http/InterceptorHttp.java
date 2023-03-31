package io.tapdata.coding.utils.http;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;

public interface InterceptorHttp {
    public HttpResponse interceptor(HttpRequest request, HttpResponse execute, boolean ignore);
}
