package com.tapdata.tm.proxyprocess.client;

import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandResult;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessHttpRequest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class ProxyProcessHttpClient {
    private final RestTemplate restTemplate;

    @Autowired
    public ProxyProcessHttpClient() {
        this(createRestTemplate());
    }

    public ProxyProcessHttpClient(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public ProxyProcessCommandResult execute(String targetUrl, String token, ProxyProcessHttpRequest request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        HttpEntity<Object> entity = new HttpEntity<>(request.getBody(), headers);
        ResponseEntity<String> response = restTemplate.exchange(
                targetUrl, HttpMethod.valueOf(request.getMethod().toUpperCase()), entity, String.class);

        ProxyProcessCommandResult result = new ProxyProcessCommandResult();
        result.setStatusCode(response.getStatusCode().value());
        Map<String, List<String>> responseHeaders = new LinkedHashMap<>();
        response.getHeaders().forEach((name, values) -> responseHeaders.put(name, new ArrayList<>(values)));
        result.setHeaders(responseHeaders);
        result.setBody(response.getBody());
        return result;
    }

    private static RestTemplate createRestTemplate() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(10_000);
        requestFactory.setReadTimeout(30_000);
        RestTemplate restTemplate = new RestTemplate(requestFactory);
        restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
            @Override
            public boolean hasError(org.springframework.http.client.ClientHttpResponse response) {
                return false;
            }
        });
        return restTemplate;
    }
}
