package io.tapdata.connector.tidb.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import io.tapdata.connector.tidb.util.pojo.Changefeed;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpUtil {
    public HttpUtil() {
    }

    CloseableHttpClient httpClient = HttpClientBuilder.create().build();

    public Boolean deleteChangefeed(String changefeedId, String cdcUrl) {
        // cdcUrl = "192.168.1.179:8300";
        String url = "http://" + cdcUrl + "/api/v1/changefeeds/" + changefeedId;
        HttpDelete httpDelete = new HttpDelete(url);
        CloseableHttpResponse response = null;
        try {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(200000)
                    .setConnectionRequestTimeout(200000)
                    .setSocketTimeout(200000)
                    .setRedirectsEnabled(true).build();
            httpDelete.setConfig(requestConfig);
            response = httpClient.execute(httpDelete);
            HttpEntity responseEntity = response.getEntity();
            System.out.println("响应状态为:" + response.getStatusLine());
            if (responseEntity != null) {
                System.out.println("响应内容长度为:" + responseEntity.getContentLength());
                System.out.println("响应内容为:" + EntityUtils.toString(responseEntity));
                System.out.println("code" + response.getStatusLine().getStatusCode());
            }
            if (response.getStatusLine().getStatusCode() == 202) {
                return true;
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpClient != null) {
                    httpClient.close();
                }
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    public Boolean createChangefeed(Changefeed changefeed, String cdcUrl) {
        Boolean flag = false;
        String url = "http://" + cdcUrl + "/api/v1/changefeeds";
        HttpPost httpPost = new HttpPost(url);
        SerializeConfig config = new SerializeConfig();
        config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
        httpPost.setEntity(new StringEntity(JSON.toJSONString(changefeed, config), "UTF-8"));
        httpPost.setHeader("Content-Type", "application/json;charset=utf8");
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            HttpEntity responseEntity = response.getEntity();
            System.out.println("响应状态为:" + response.getStatusLine());
            if (responseEntity != null) {
                System.out.println("响应内容长度为:" + responseEntity.getContentLength());
                System.out.println("响应内容为:" + EntityUtils.toString(responseEntity));
                System.out.println("code" + response.getStatusLine().getStatusCode());
            }
            if (response.getStatusLine().getStatusCode() == 202) {
                flag = true;
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }
}


