package com.tapdata.tm.modules.dto;

import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.deepoove.poi.plugin.highlight.HighlightRenderData;
import com.deepoove.poi.plugin.highlight.HighlightStyle;
import com.google.gson.Gson;
import org.apache.commons.codec.CharEncoding;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.helper.StringUtil;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ApiViewUtil {
    public static final String ACCESS_TOKEN="eyJraWQiOiJjOWVmMmVkMS0xYTYxLTQ4ODQtYWJmYS01YjVjMzZiMWYwNjYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjbGllbnRJZCI6IjVjMGU3NTBiN2E1Y2Q0MjQ2NGE1MDk5ZCIsInJvbGVzIjpbIiRldmVyeW9uZSIsImFkbWluIl0sImlzcyI6Imh0dHA6XC9cLzEyNy4wLjAuMTozMDAwIiwiZXhwaXJlZGF0ZSI6MTY5MDI1MjAzMjA5MiwiYXVkIjoiNWMwZTc1MGI3YTVjZDQyNDY0YTUwOTlkIiwiY3JlYXRlZEF0IjoxNjg5MDQyNDMyMDkyLCJuYmYiOjE2ODkwNDI0MzIsInNjb3BlIjpbIjViOWEwYTM4M2ZjYmEwMjY0OTUyNGJmMSJdLCJleHAiOjE2OTAyNTIwMzIsImlhdCI6MTY4OTA0MjQzMiwianRpIjoiZjcwY2E1NDktYzg0Zi00YTI2LWJkOWItM2IyMjNkMzAxNGZjIn0.NZhs9U6WaebP0GTwH3vBc4VrndzS9Abtko3nV61y5Tvw6AdI3nMvzeYZ10MNSdRUU2E1Tm5Vq-KMkzCtoa360KXgg-BC-yH7vS8aKl6RjXIfV5RNEpPUo6sTEWS4MqJJWxikWxq8jrUABGF8xcBu4ptvfh9TOu3K3VuTtZeYEVcwEn3sk5xMqqd0Z1j9-EuZpFcHG2pBm30YzNVbHcwycSMqDdRn--6Rn-eUhk8ifhroFdZBgcLAlOcZJEuQ7oEnG7sNNAxqxSkubPL9sg-X1w80NdydKtvnpotXs19gIhxaMXq7WL6bI1_19ECTnelUDWft4neNm01yO7KR_wWeqw";
    public static ApiView convert(Map<String, List<ModulesDto>> modules,String ip){
        ApiView apiView = new ApiView();
        List<ApiType> apiTypes = new ArrayList<>();
        AtomicInteger index= new AtomicInteger();
        modules.keySet().forEach((s) -> {
            ApiType apiType = new ApiType();
            apiType.setApiTypeName(s);
            List<ApiModule> allModules = new ArrayList<>();
            index.getAndIncrement();
            modules.get(s).forEach(modulesDto -> {
                ApiModule module = new ApiModule();
                module.setName(modulesDto.getName());
                module.setApiTypeIndex(Integer.valueOf(index.get()));
                module.setIp(ip);
                module.setPath(modulesDto.getPaths().get(0).getPath());
                module.setDescription(modulesDto.getDescription());
                module.setFields(modulesDto.getPaths().get(0).getFields());
                module.setParams(modulesDto.getPaths().get(0).getParams());
                StringBuilder urlBuilder=new StringBuilder();
                urlBuilder
                        .append(module.getIp())
                        .append(module.getPath())
                        .append("?access_token=")
                        .append(ACCESS_TOKEN.substring(0,8))
                        .append("&limit=1&page=1");
                String requestUrl = urlBuilder.toString();
                module.setRequestString(requestUrl);
                String testResult = doGet(module.getIp()+module.getPath());
                HighlightRenderData responseRender = new HighlightRenderData();
                if(!StringUtil.isBlank(testResult)){
                    JSONObject jsonObject = JSONObject.parseObject(testResult);
                    String jsonFormatString = JSON.toJSONString(jsonObject, SerializerFeature.PrettyFormat,
                            SerializerFeature.WriteMapNullValue,
                            SerializerFeature.WriteDateUseDateFormat);
                    responseRender.setCode(jsonFormatString);
                }else{
                    responseRender.setCode("测试结果失败或为空");
                }
                responseRender.setLanguage("json");
                module.setCode(responseRender);
                allModules.add(module);
            });
            apiType.setApiList(allModules);
            apiTypes.add(apiType);
        });
        apiView.setApiTypeList(apiTypes);
        return apiView;
    }
    public static String doGet(String url) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            StringBuilder urlBuilder = new StringBuilder();
            urlBuilder.append(url);
            urlBuilder.append("?access_token=").append(ACCESS_TOKEN);
            urlBuilder.append("&").append("&limit=1&page=1");
            HttpGet httpGet = new HttpGet(urlBuilder.toString());
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .setSocketTimeout(15000).build();
            httpGet.setConfig(requestConfig);
            CloseableHttpResponse response = httpClient.execute(httpGet);
            StatusLine statusLine = response.getStatusLine();
            int statusCode = statusLine.getStatusCode();
            if (statusCode < HttpStatus.SC_OK || statusCode >= HttpStatus.SC_MULTIPLE_CHOICES) {
                return "";
            }
            HttpEntity entity = response.getEntity();
            if (entity != null)
                return EntityUtils.toString(entity, CharEncoding.UTF_8);
            else
                return "";
        } catch (IOException e) {
            return "";
        }
    }

}
