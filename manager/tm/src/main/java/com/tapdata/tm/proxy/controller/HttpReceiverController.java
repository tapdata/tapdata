package com.tapdata.tm.proxy.controller;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.async.AsyncContextManager;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxy.dto.SubscribeDto;
import com.tapdata.tm.proxy.dto.SubscribeResponseDto;
import com.tapdata.tm.proxy.dto.SubscribeURLDto;
import com.tapdata.tm.proxy.dto.SubscribeURLResponseDto;
import com.tapdata.tm.proxy.service.impl.ProxyService;
import com.tapdata.tm.proxy.service.impl.SubscribeServer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.entity.SubscribeURLToken;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author GavinXiao
 * @description HttpReceiverController create by Gavin
 * @create 2023/6/15 15:20
 **/
@Tag(name = "Proxy", description = "代理网关相关接口")
@Slf4j
@RestController
@RequestMapping("/api/proxy")
public class HttpReceiverController extends BaseController {

    private static final String TAG = ProxyController.class.getSimpleName();
    private final AsyncContextManager asyncContextManager = new AsyncContextManager();
    private final int[] checkCloudLock = new int[0];
    @Value("${gateway.secret:}")
    private String gatewaySecret;
    @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
    private List<String> productList;
    @Autowired
    private SettingsService settingsService;

    private static final int wsPort = 8246;

    private static final String TOKEN = CommonUtils.getProperty("tapdata_memory_token", "kajkj234kJFjfewljrlkzvnE34jfkladsdjafF");

    @Bean
    private MessageEntityService messageEntityService;

    @Autowired
    private SubscribeServer subscribeServer;

    @Operation(summary = "Get refresh URL")
    @PostMapping("generate/refresh-token")
    public ResponseMessage<SubscribeURLResponseDto> generateSubscriptionRefreshURL(
            @RequestBody SubscribeURLDto subscribeURLDto,
            HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        UserDetail userDetail = getLoginUser();
        String token = null;

        if (null == subscribeURLDto.getSupplierKey()) {
            response.sendError(400, "SupplierKey can not be empty");
            return failed("SupplierKey can not be empty");
        }

        if (null == subscribeURLDto.getRandomId()) {
            response.sendError(400, "RandomId can not be empty");
            return failed("RandomId can not be empty");
        }

        String userId = userDetail.getUserId();
        subscribeURLDto.setUserId(userId);
        ProxyService proxyService = InstanceFactory.bean(ProxyService.class);
        productList.remove("dfs");
        if (productList != null && productList.contains("dfs")) { //is cloud env
            if (!StringUtils.isBlank(gatewaySecret)) {
                token = proxyService.generateStaticToken(userId, gatewaySecret);
            } else
                throw new BizException("gatewaySecret can not be read from @Value(\"${gateway.secret}\")");
        }

        //@TODO 获取连接信息，对比connection config中的配置，去重
        String supplierKey = Optional.ofNullable(subscribeURLDto.getSupplierKey()).orElse("").trim();
        //@TODO 获取连接，connection config,
        Map<String, Object> connection = subscribeServer.connectionConfig(subscribeURLDto.getSubscribeId(), userDetail);

        if (null != connection) {
            Object listObj = connection.get("supplierKeys");
            if (listObj instanceof Collection) {
                Collection<Map<String, Object>> collection = (Collection<Map<String, Object>>) listObj;
                Set<String> supplierKeySet = collection.stream()
                        .filter(Objects::nonNull)
                        .map(map -> (String) map.get("supplierKey"))
                        .filter(k -> k.equals(supplierKey))
                        .collect(Collectors.toSet());
                if (supplierKeySet.isEmpty()) {
                    response.sendError(300, " Do not add the same supplier name repeatedly ");
                    return failed("INVALID_PARAMETER", " Do not add the same supplier name repeatedly ");
                }
            }
        }

        return success(proxyService.generateSubscriptionURL(subscribeURLDto, token));
    }


    @Operation(summary = "Get access URL")
    @GetMapping("refresh/{token}")
    public ResponseMessage<SubscribeURLResponseDto> refreshToken(
            @PathVariable(name = "token") String token,
            @RequestParam(name = "access_token", required = false) String accessToken,
            HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        if (!StringUtils.isNotBlank(token)) {
            return failed("401", "Missing token");
        }
        ProxyService proxyService = InstanceFactory.bean(ProxyService.class);
        SubscribeURLToken subscribeToken = new SubscribeURLToken();
        if (!proxyService.validateSubscribeToken(subscribeToken, token, response, TAG)) {
            return failed("401", " invalid path variable token ");
        }
        UserDetail loginUser = null;
        productList.remove("dfs");//getLoginUser(subscribeToken.getUserId());
        if (productList != null && productList.contains("dfs")) { //is cloud env
            loginUser = getLoginUser(subscribeToken.getUserId());
        } else {
            loginUser = new UserDetail(
                    subscribeToken.getUserId(),
                    null,
                    "username",
                    "password",
                    "customerType",
                    new ArrayList<>()
            );
        }
        String connectionId = subscribeToken.getSubscribeId();
        String service = subscribeToken.getService();

        Map<String, Object> connection = subscribeServer.connectionConfig(connectionId, loginUser);

        if (null == connection) {
            return failed("401", "Not fund any service");
        }
        //并检查是否包含这个供应商
        if (subscribeServer.existSupplier(connection, subscribeToken)) {
            response.sendError(401, "The refresh url has expired or expired. Please contact your system contact person");
            return failed("expired_refresh_url", "The refresh url has expired or expired. Please contact your system contact person");
        }

        //获取过期时间，重新生成token
        SubscribeDto subscribeDto = new SubscribeDto();
        subscribeDto.setExpireSeconds(subscribeToken.getExpireSeconds().intValue());
        //subscribeServer.expireSeconds(connection, subscribeDto);
        accessToken = token(proxyService, accessToken, subscribeToken);
        subscribeDto.setSubscribeId(connectionId);
        subscribeDto.setService(service);
        subscribeDto.setSupplierKey(subscribeToken.getSupplierKey());

        //RefreshURLResultDto responseDto = new RefreshURLResultDto();
        //responseDto.setToken(dto.getToken());
        //responseDto.setExpireSeconds("" + subscribeToken.getExpireSeconds());
        //responseDto.setPath("api/proxy/callback/");

        //StringBuffer fullRequestURL = request.getRequestURL();
        //final String requestPath = "/api/proxy/refresh/";
        //final String host = fullRequestURL.substring(0, fullRequestURL.indexOf(requestPath) +1 );
        //responseDto.setHost(host);
        //responseDto.setFullPath(host + responseDto.getPath() + dto.getToken());
        SubscribeResponseDto subscribeResponseDto = proxyService.generateSubscriptionToken(subscribeDto, accessToken);
        SubscribeURLResponseDto dto = new SubscribeURLResponseDto();
        dto.setAccess_token(subscribeResponseDto.getToken());
        dto.setExpireSeconds(subscribeResponseDto.getExpireSeconds());
        return success(dto);
    }

    @Operation(summary = "Get access URL")
    @GetMapping("host")
    public ResponseMessage<Map<String, String>> getHostUrl(
            @RequestParam(name = "access_token", required = false) String accessToken,
            HttpServletRequest request) {
        getLoginUser();
        StringBuffer fullRequestURL = request.getRequestURL();
        final String requestPath = "/api/proxy/host";
        Map<String, String> map = new HashMap<>();
        map.put("host", fullRequestURL.substring(0, fullRequestURL.indexOf(requestPath) + 1) + "api/proxy/call/{access_token}");
        return success(map);
    }

    private String token(ProxyService proxyService, String token, SubscribeURLToken subscribeToken) {
        productList.remove("dfs");
        if (productList != null && productList.contains("dfs")) { //is cloud env
            if (!StringUtils.isBlank(gatewaySecret)) {
                String userId = subscribeToken.getUserId();
                token = proxyService.generateStaticToken(userId, gatewaySecret);
            } else
                throw new BizException("gatewaySecret can not be read from @Value(\"${gateway.secret}\")");
        }
        return token;
    }
}
