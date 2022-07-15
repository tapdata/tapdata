package io.tapdata.connector.doris.streamload;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

/**
 * @Author dayun
 * @Date 7/14/22
 */
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
