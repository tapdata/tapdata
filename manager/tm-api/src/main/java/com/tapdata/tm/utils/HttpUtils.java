package com.tapdata.tm.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

@Slf4j
public class HttpUtils {
    private static final String UTF_8 ="utf-8";


    /**
     * get请求传输数据
     *
     * @return
     * @throws ClientProtocolException
     * @throws IOException
     */
    public static String sendGetData(String path, Map<String, String> headMap) {
        log.info("request tcm, path：{}，headMap：{}  ",path,headMap);
        String result = "";
        CloseableHttpResponse response =null;
        try(CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // 创建get方式请求对象
            HttpGet httpGet = new HttpGet(path);
            if (null != headMap) {
                for (Map.Entry<String,String> entry : headMap.entrySet()) {
                    httpGet.addHeader(entry.getKey(), entry.getValue());
                }
            }
            // 通过请求对象获取响应对象
            response = httpClient.execute(httpGet);

            // 获取结果实体
            // 判断网络连接状态码是否正常(0--200都数正常)
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                result = EntityUtils.toString(response.getEntity(), UTF_8);
            } else {
                log.error("get请求传输 异常.url:{}, headMap:{}", path, headMap);
            }
        } catch (ClientProtocolException e) {
            log.error("get请求传输 异常 ", e);
            log.error("get请求传输 异常.path:{}, headMap:{}", path, headMap);
        } catch (IOException e) {
            log.error("get请求传输 异常 ", e);
            log.error("get请求传输 异常.path:{}, headMap:{}", path, headMap);
        }
        finally {
            try {
                if (null!=response){
                    response.close();
                }
            } catch (IOException e) {
                log.error("关闭response 异常 ", e);
            }
        }
        log.debug(result);
        return result;
    }

    public static String sendPostData(String path, String bodyJson,String userId) {
        log.info("request tcm, path：{}，bodyJson：{}  ",path,bodyJson);
        String result = "";
        CloseableHttpResponse response =null;
        try(CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // 创建httpclient对象
            StringEntity entity = new StringEntity(bodyJson, Charset.forName("UTF-8"));
            entity.setContentType("application/json");
            // 创建post方式请求对象
            HttpPost httpPost = new HttpPost(path);
            httpPost.addHeader("user_id",userId);
            httpPost.setEntity(entity);
            // 通过请求对象获取响应对象
            response = httpClient.execute(httpPost);
            // 获取结果实体
            // 判断网络连接状态码是否正常(0--200都数正常)
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                result = EntityUtils.toString(response.getEntity(), UTF_8);
            } else {
                log.error("post请求传输 异常.url:{}, bodyJson:{}", path, bodyJson);
            }


        } catch (ClientProtocolException e) {
            log.error("post请求传输 异常 ", e);
            log.error("post请求传输 异常.path:{}, headMap:{}", path, bodyJson);
        } catch (IOException e) {
            log.error("post请求传输 异常 ", e);
            log.error("post请求传输 异常.path:{}, headMap:{}", path, bodyJson);
        }
        finally {
            try {
                // 释放链接
                if (null!=response){
                    response.close();
                }
            } catch (IOException e) {
                log.error("关闭response 异常 ", e);
            }
        }
        log.debug(result);
        return result;
    }

    public static String sendPostData(String path, String bodyJson) {
        log.info("request tcm, path：{}，bodyJson：{}  ",path,bodyJson);
        String result = "";
        CloseableHttpResponse response =null;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()){
            // 创建httpclient对象
            StringEntity entity = new StringEntity(bodyJson, Charset.forName("UTF-8"));
            entity.setContentType("application/json");
            // 创建post方式请求对象
            HttpPost httpPost = new HttpPost(path);
            httpPost.addHeader("Token","cba0125db7a18a32508a4e9e077058f33352c1c9124d2c3cbeb3f426096f100a");
            httpPost.setEntity(entity);
            // 通过请求对象获取响应对象
            response = httpClient.execute(httpPost);
            // 获取结果实体
            // 判断网络连接状态码是否正常(0--200都数正常)
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                result = EntityUtils.toString(response.getEntity(), UTF_8);
            } else {
                log.error("post请求传输 异常.url:{}, bodyJson:{}", path, bodyJson);
            }


        } catch (ClientProtocolException e) {
            log.error("post请求传输 异常 ", e);
            log.error("post请求传输 异常.path:{}, headMap:{}", path, bodyJson);
        } catch (IOException e) {
            log.error("post请求传输 异常 ", e);
            log.error("post请求传输 异常.path:{}, headMap:{}", path, bodyJson);
        }
        finally {
            try {
                // 释放链接
                if (null!=response){
                    response.close();
                }
            } catch (IOException e) {
                log.error("关闭response 异常 ", e);
            }
        }
        log.debug(result);
        return result;
    }

}
