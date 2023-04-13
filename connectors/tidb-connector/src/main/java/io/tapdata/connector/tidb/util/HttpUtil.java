package io.tapdata.connector.tidb.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.kit.ErrorKit;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpUtil implements AutoCloseable {

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

    public Boolean createChangefeed(ChangeFeed changefeed, String cdcUrl) throws IOException {
        String url = "http://" + cdcUrl + "/api/v1/changefeeds";
        HttpPost httpPost = new HttpPost(url);
        SerializeConfig config = new SerializeConfig();
        config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
        httpPost.setEntity(new StringEntity(JSON.toJSONString(changefeed, config), "UTF-8"));
        httpPost.setHeader("Content-Type", "application/json;charset=utf8");
        try (
                CloseableHttpResponse response = httpClient.execute(httpPost)
        ) {
            HttpEntity responseEntity = response.getEntity();
            System.out.println("响应状态为:" + response.getStatusLine());
            if (responseEntity != null) {
                System.out.println("响应内容长度为:" + responseEntity.getContentLength());
                System.out.println("响应内容为:" + EntityUtils.toString(responseEntity));
                System.out.println("code"+response.getStatusLine().getStatusCode());
            }
            if (response.getStatusLine().getStatusCode() == 202) {
                isChangeFeedClosed = false;
                return true;
            }
        }
        return false;
    }
public  Boolean resumeChangefeed(String changefeedId,String cdcUrl)throws IOException{
    String url ="http://"+ cdcUrl+"/api/v1/changefeeds/"+changefeedId+"/resume";
    HttpPost httpPost = new HttpPost(url);
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
    public  Boolean pauseChangefeed(String changefeedId,String cdcUrl)throws IOException{
        String url ="http://"+ cdcUrl+"/api/v1/changefeeds/"+changefeedId+"/pause";
        HttpPost httpPost = new HttpPost(url);
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


