package com.tapdata.tm.modules.dto;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.deepoove.poi.plugin.highlight.HighlightRenderData;
import com.tapdata.tm.commons.util.ThrowableUtils;
import com.tapdata.tm.module.dto.ModulesDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.CharEncoding;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.internal.StringUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class ApiViewUtil {
    public static final String PARAMS="?limit=1&page=1";
    public static ApiView convert(Map<String, List<ModulesDto>> modules, String ip){
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
                String requestUrl = module.getIp() + module.getPath() + PARAMS
                        + "\nAuthorization: Bearer <token>";
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
            HttpGet httpGet = new HttpGet(url + PARAMS);
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
                    .setConnectionRequestTimeout(5000)
                    .setSocketTimeout(15000).build();
            httpGet.setConfig(requestConfig);
            CloseableHttpResponse response = httpClient.execute(httpGet);
            StatusLine statusLine = response.getStatusLine();
            int statusCode = statusLine.getStatusCode();
            if (statusCode < HttpStatus.SC_OK || statusCode >= HttpStatus.SC_MULTIPLE_CHOICES) {
                log.info("request status is not ok");
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
