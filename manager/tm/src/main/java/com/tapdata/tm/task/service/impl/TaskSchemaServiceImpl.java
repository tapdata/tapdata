package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.TaskSchemaService;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.utils.CommonUtil;
import com.tapdata.tm.utils.MockRequest;
import com.tapdata.tm.utils.OEMReplaceUtil;
import com.tapdata.tm.utils.ResultMapUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;
@Service
public class TaskSchemaServiceImpl implements TaskSchemaService {


    @Override
    public Map<String, Object> discoverSchemaMQ(Node node, Map<String, Object> nodeConfig, String taskId,List<String> tableName) {
        String serverPort = CommonUtils.getProperty("tapdata_proxy_server_port", "3000");
        int port;
        try {
            port = Integer.parseInt(serverPort);
        } catch (Exception exception){
            return ResultMapUtil.resultMap(taskId, false, "Can't get server port.");
        }
        Map.Entry<String, Map<String, Object>> attributes = getLoginUserAttributes();
        String attributesKey = attributes.getKey();
        Map<String, Object> attributesValue = attributes.getValue();
        String url = "http://localhost:" +
                port +"/api/proxy/call" +
                ("Param".equals(attributesKey) ? "?access_token=" + attributesValue.get("access_token")  : "");
        Map<String, Object> paraMap = new HashMap<>();
        paraMap.put("className", "DiscoverSchemaService");
        paraMap.put("method", "discoverSchemaMQ");
        String connectionId = ((DataParentNode) node).getConnectionId();

        paraMap.put("args", new ArrayList<Object>() {{
            add(connectionId);
            add(node.getTaskId());
            add(nodeConfig);
            add(tableName);
        }});
        try {
            OkHttpClient client = new OkHttpClient.Builder()
                    .connectTimeout(90, TimeUnit.SECONDS)
                    .readTimeout(90, TimeUnit.SECONDS)
                    .build();
            Request.Builder post = new Request.Builder()
                    .url(url)
                    .method("POST", RequestBody.create(MediaType.parse("application/json"), JsonUtil.toJsonUseJackson(paraMap)))
                    .addHeader("Content-Type", "application/json");
            if ("Header".equals(attributesKey) && null != attributesValue && !attributesValue.isEmpty()){
                for (Map.Entry<String, Object> entry : attributesValue.entrySet()) {
                    post.addHeader(entry.getKey(), String.valueOf(entry.getValue()));
                }
            }
            Request request = post.build();
            Call call = client.newCall(request);
            Response response = call.execute();
            int code = response.code();
            return 200 >= code && code < 300 ?
                    (Map<String, Object>) fromJson(OEMReplaceUtil.replace(response.body().string(), "connector/replace.json"))
                    : ResultMapUtil.resultMap(taskId, false, "Access remote service error, http code: " + code);
        }catch (Exception e){
            return ResultMapUtil.resultMap(taskId, false, e.getMessage());
        }
    }

    private Map.Entry<String, Map<String, Object>> getLoginUserAttributes() {
        MockRequest mockRequest = CommonUtil.getRequest();
        if (null != mockRequest) {
            String userId = mockRequest.getUserId();
            String authorization = mockRequest.getAuthorization();
            String queryString = mockRequest.getQueryString();
            return getLoginUserAttributesMap(userId,queryString,authorization);
        } else {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            HttpServletRequest request = attributes.getRequest();
            String userIdFromHeader = request.getHeader("user_id");
            String queryString =request.getQueryString();
            String authorization =request.getHeader("authorization");
            return getLoginUserAttributesMap(userIdFromHeader, queryString, authorization);
        }
    }

    @NotNull
    private static AbstractMap.SimpleEntry<String, Map<String, Object>> getLoginUserAttributesMap(String userIdFromHeader, String queryString, String authorization) {
        Map<String, Object> ent = new HashMap<>();
        if (!com.tapdata.manager.common.utils.StringUtils.isBlank(userIdFromHeader)) {
            ent.put("user_id", userIdFromHeader);
            return new AbstractMap.SimpleEntry<>("Header", ent);
        } else if ((queryString != null ? queryString : "").contains("access_token")) {
            String accessToken = getAccessToken(queryString);
            ent.put("access_token", accessToken);
            return new AbstractMap.SimpleEntry<>("Param", ent);
        } else if (authorization != null) {
            ent.put("authorization", authorization.trim());
            return new AbstractMap.SimpleEntry<>("Header", ent);
        } else {
            throw new BizException("NotLogin");
        }
    }

    private static String getAccessToken(String queryString) {
        Map<String, String> queryMap = Arrays.stream(queryString.split("&"))
                .filter(s -> s.startsWith("access_token"))
                .map(s -> s.split("=")).collect(Collectors.toMap(a -> a[0], a -> {
                    try {
                        return URLDecoder.decode(a[1], "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return a[1];
                    }
                }, (a, b) -> a));
        String accessToken = queryMap.get("access_token");
        return accessToken;
    }
}
