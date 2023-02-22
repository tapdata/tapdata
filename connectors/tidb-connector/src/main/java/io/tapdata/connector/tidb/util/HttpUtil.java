package io.tapdata.connector.tidb.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import io.tapdata.connector.tidb.util.pojo.Changefeed;
import io.tapdata.kit.ErrorKit;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;

public class HttpUtil {

    private final CloseableHttpClient httpClient;
    private boolean isChangeFeedClosed;

    public HttpUtil() {
        httpClient = HttpClientBuilder.create().build();
    }

    public Boolean deleteChangefeed(String changefeedId, String cdcUrl) throws IOException {
        // cdcUrl = "192.168.1.179:8300";
        String url = "http://" + cdcUrl + "/api/v1/changefeeds/" + changefeedId;
        HttpDelete httpDelete = new HttpDelete(url);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(200000)
                .setConnectionRequestTimeout(200000)
                .setSocketTimeout(200000)
                .setRedirectsEnabled(true).build();
        httpDelete.setConfig(requestConfig);
        try (
                CloseableHttpResponse response = httpClient.execute(httpDelete)
        ) {
            if (response.getStatusLine().getStatusCode() == 202) {
                isChangeFeedClosed = true;
                return true;
            }
        }
        return false;
    }

    public Boolean createChangefeed(Changefeed changefeed, String cdcUrl) throws IOException {
        String url = "http://" + cdcUrl + "/api/v1/changefeeds";
        HttpPost httpPost = new HttpPost(url);
        SerializeConfig config = new SerializeConfig();
        config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
        httpPost.setEntity(new StringEntity(JSON.toJSONString(changefeed, config), "UTF-8"));
        httpPost.setHeader("Content-Type", "application/json;charset=utf8");
        try (
                CloseableHttpResponse response = httpClient.execute(httpPost)
        ) {
            if (response.getStatusLine().getStatusCode() == 202) {
                isChangeFeedClosed = false;
                return true;
            }
        }
        return false;
    }

    public void close() {
        ErrorKit.ignoreAnyError(httpClient::close);
    }

    public boolean isChangeFeedClosed() {
        return isChangeFeedClosed;
    }
}


