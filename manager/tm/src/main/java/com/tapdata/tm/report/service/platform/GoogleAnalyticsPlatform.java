package com.tapdata.tm.report.service.platform;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GoogleAnalyticsPlatform implements ReportPlatform{
    public static final String CONNECT_TIMEOUT = System.getenv().getOrDefault("GOOGLE_CLIENT_CONNECT_TIMEOUT","5000");
    public static final String SOCKET_TIMEOUT = System.getenv().getOrDefault("GOOGLE_CLIENT_SOCKET_TIMEOUT","5000");
    public static final String CONNECTION_REQUEST_TIMEOUT = System.getenv().getOrDefault("GOOGLE_CLIENT_CONNECTION_REQUEST_TIMEOUT","5000");
    @Value("${report.url.measurementId}")
    private String measurementId;
    @Value("${report.url.apiSecret}")
    private String apiSecret;
    @Value("${report.url.clientId}")
    private String clientId;
    private final CloseableHttpClient client;

    protected CloseableHttpClient getClient() {
        return client;
    }

    public GoogleAnalyticsPlatform(){
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(processParamIfNeed(CONNECT_TIMEOUT))
                .setSocketTimeout(processParamIfNeed(SOCKET_TIMEOUT))
                .setConnectionRequestTimeout(processParamIfNeed(CONNECTION_REQUEST_TIMEOUT))
                .build();

        HttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(3, true);
        client = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .evictExpiredConnections()
                .setRetryHandler(retryHandler)
                .build();
    }

    @Override
    public void sendRequest(String eventName, String params) {
        String url = "https://www.google-analytics.com/mp/collect?measurement_id=" + measurementId + "&api_secret=" + apiSecret;

        // build JSON data
        String jsonBody = "{\n" +
                "    \"client_id\": \"%s\",\n" +
                "    \"events\": [{\n" +
                "        \"name\": \"%s\",\n" +
                "        \"params\": %s\n" +
                "    }]\n" +
                "}";
        try {
            jsonBody = String.format(jsonBody, clientId, eventName, params);
            HttpPost httpPost = new HttpPost(url);
            StringEntity entity = new StringEntity(jsonBody);
            httpPost.setEntity(entity);
            httpPost.setHeader("Content-Type", "application/json");
            client.execute(httpPost);
        } catch (Exception e) {
            log.info("report user data failed", e);
        }
    }
    protected int processParamIfNeed(String value){
        int param = 5000;
        try {
            return Integer.parseInt(value);
        }catch (Exception e){
            return param;
        }
    }
}
