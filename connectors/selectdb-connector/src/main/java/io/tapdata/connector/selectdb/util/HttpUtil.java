package io.tapdata.connector.selectdb.util;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

/**
 * Author:Skeet
 * Date: 2022/12/9
 **/
public class HttpUtil {
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });

    public CloseableHttpClient getHttpClient() {
        return httpClientBuilder.build();
    }
}
