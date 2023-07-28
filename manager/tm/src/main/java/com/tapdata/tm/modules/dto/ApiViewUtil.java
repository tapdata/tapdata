package com.tapdata.tm.modules.dto;

import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.deepoove.poi.plugin.highlight.HighlightRenderData;
import com.deepoove.poi.plugin.highlight.HighlightStyle;
import com.google.gson.Gson;
import com.tapdata.tm.commons.util.ThrowableUtils;
import lombok.extern.slf4j.Slf4j;
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


@Slf4j
public class ApiViewUtil {
   public static final String PARAMS="&limit=1&page=1";
    public static final String PREFIX_ACCESS_TOKEN="?access_token=";
    public static ApiView convert(Map<String, List<ModulesDto>> modules,String ip,String apiToken){
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
                        .append(PREFIX_ACCESS_TOKEN)
                        .append(apiToken.substring(0,8))
                        .append(PARAMS);
                String requestUrl = urlBuilder.toString();
                module.setRequestString(requestUrl);
                String testResult = doRequest(module.getIp()+module.getPath(),apiToken);
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
    public static String doRequest(String url,String apiToken) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            StringBuilder urlBuilder = new StringBuilder();
            urlBuilder.append(url);
            urlBuilder.append(PREFIX_ACCESS_TOKEN).append(apiToken);
            urlBuilder.append(PARAMS);
            HttpGet httpGet = new HttpGet(urlBuilder.toString());
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .setSocketTimeout(15000).build();
            httpGet.setConfig(requestConfig);
            CloseableHttpResponse response = httpClient.execute(httpGet);
            StatusLine statusLine = response.getStatusLine();
            int statusCode = statusLine.getStatusCode();
            if (statusCode < HttpStatus.SC_OK || statusCode >= HttpStatus.SC_MULTIPLE_CHOICES) {
                log.info("request status is not ok,status is {}",statusCode);
                return "";
            }
            HttpEntity entity = response.getEntity();
            if (entity != null)
                return EntityUtils.toString(entity, CharEncoding.UTF_8);
            else
                log.info("request entity is empty");
                return "";
        } catch (IOException e) {
            log.error("request error{}", ThrowableUtils.getStackTraceByPn(e));
            return "";
        }
    }

}
