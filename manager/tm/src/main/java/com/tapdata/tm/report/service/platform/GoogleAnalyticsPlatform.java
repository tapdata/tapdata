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
                .setConnectTimeout(5000)
                .setSocketTimeout(5000)
                .setConnectionRequestTimeout(5000)
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
}
