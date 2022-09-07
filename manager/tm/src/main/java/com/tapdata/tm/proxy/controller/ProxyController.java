package com.tapdata.tm.proxy.controller;

import cn.hutool.crypto.digest.MD5;
import com.tapdata.tm.async.AsyncContextManager;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxy.dto.LoginProxyDto;
import com.tapdata.tm.proxy.dto.LoginProxyResponseDto;
import com.tapdata.tm.proxy.dto.SubscribeDto;
import com.tapdata.tm.proxy.dto.SubscribeResponseDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.service.CommandExecutionService;
import io.tapdata.modules.api.net.service.EventQueueService;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.core.utils.JWTUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.simplify.TapSimplify.toJson;


@Tag(name = "Proxy", description = "代理网关相关接口")
@Slf4j
@RestController
@RequestMapping("/api/proxy")
public class ProxyController extends BaseController {
    private final AsyncContextManager asyncContextManager = new AsyncContextManager();
    private static final String key = "asdfFSDJKFHKLASHJDKQJWKJehrklHDFJKSMhkj3h24jkhhJKASDH723ty4jkhasdkdfjhaksjdfjfhJDJKLHSAfadsf";
    private static final int wsPort = 8246;
    /**
     *
     * @return
     */
    @Operation(summary = "Generate jwt token")
    @PostMapping()
    public ResponseMessage<LoginProxyResponseDto> generateTokenForEngineToProxy(@RequestBody LoginProxyDto loginProxyDto, HttpServletRequest request) {
        if(loginProxyDto == null || loginProxyDto.getClientId() == null || loginProxyDto.getService() == null || loginProxyDto.getTerminal() == null)
            throw new BizException("loginDto is illegal, " + loginProxyDto);
        if(StringUtils.isEmpty(loginProxyDto.getService()))
            throw new BizException("Illegal service for generating token");
        UserDetail userDetail = getLoginUser();

        LoginProxyResponseDto loginProxyResponseDto = new LoginProxyResponseDto();
        String token = JWTUtils.createToken(key,
                map(
                        entry("service", loginProxyDto.getService().toLowerCase()),
                        entry("clientId", loginProxyDto.getClientId()),
                        entry("terminal", loginProxyDto.getTerminal()),
                        entry("uid", userDetail.getUserId()),
                        entry("cid", userDetail.getCustomerId())
                ), 30000L);
        loginProxyResponseDto.setToken(token);
        loginProxyResponseDto.setWsPort(wsPort);
        /**
         * Use the ws url to do the load balancing.
         * Nginx for example
         *
         * upstream somestream {
         *   consistent_hash $request_uri;
         *   server 10.50.1.3:11211;
         *   server 10.50.1.4:11211;
         *   server 10.50.1.5:11211;
         * }
         */
        loginProxyResponseDto.setWsPath(loginProxyDto.getService() + "/" + MD5.create().digestHex(loginProxyDto.getClientId()));
        return success(loginProxyResponseDto);
    }
    @Operation(summary = "Generate callback url token")
    @PostMapping("subscribe")
    public ResponseMessage<SubscribeResponseDto> generateSubscriptionToken(@RequestBody SubscribeDto subscribeDto, HttpServletRequest request) {
        if(subscribeDto == null)
            throw new BizException("SubscribeDto is null");
        if(subscribeDto.getSubscribeId() == null)
            throw new BizException("SubscribeId is null");
        if(subscribeDto.getService() == null)
            subscribeDto.setService("engine");
        if(subscribeDto.getExpireSeconds() == null)
            throw new BizException("SubscribeDto expireSeconds is null");
        UserDetail userDetail = getLoginUser(); //only for check access_token
        String token = JWTUtils.createToken(key,
                map(
                        entry("service", subscribeDto.getService().toLowerCase()),
                        entry("subscribeId", subscribeDto.getSubscribeId())
                ), subscribeDto.getExpireSeconds() * 1000);

        SubscribeResponseDto subscribeResponseDto = new SubscribeResponseDto();
        subscribeResponseDto.setToken(token);
        return success(subscribeResponseDto);
    }
    @Operation(summary = "External callback url")
    @PostMapping("callback/{token}")
    public ResponseMessage<Void> rawDataCallback(@PathVariable("token") String token, @RequestBody Map<String, Object> content, HttpServletRequest request) {
        if(content == null)
            throw new BizException("content is illegal, " + null);
        Map<String, Object> claims = JWTUtils.getClaims(key, token);
        String service = (String) claims.get("service");
        String subscribeId = (String) claims.get("subscribeId");
        if(service == null || subscribeId == null) {
            throw new BizException("Illegal arguments for subscribeId {}, subscribeId {}", service, subscribeId);
        }

        EventQueueService eventQueueService = InstanceFactory.instance(EventQueueService.class, "sync");
        if(eventQueueService != null) {
            MessageEntity message = new MessageEntity().content(content).time(new Date()).subscribeId(subscribeId).service(service);
            eventQueueService.offer(message);
        }

        return success();
    }

    @Operation(summary = "External callback url")
    @PostMapping("command")
    public void command(@RequestBody CommandInfo commandInfo, HttpServletRequest request, HttpServletResponse response) {
        if(commandInfo == null)
            throw new BizException("commandInfo is illegal");

//        CommandInfo commandInfo = fromJson(body, CommandInfo.class);
        commandInfo.setId(UUID.randomUUID().toString().replace("-", ""));
        CommandExecutionService commandExecutionService = InstanceFactory.instance(CommandExecutionService.class);
        if(commandExecutionService == null) {
            throw new BizException("commandExecutionService is null");
        }
        asyncContextManager.registerAsyncJob(commandInfo.getId(), request, (result, error) -> {
            String responseStr;
            if(error != null) {
                int code = NetErrors.UNKNOWN_ERROR;
                if(error instanceof CoreException) {
                    CoreException coreException = (CoreException) error;
                    code = coreException.getCode();
                }
                responseStr =
                                "           {\n" +
                                "                \"reqId\": \"" + UUID.randomUUID() + "\",\n" +
                                "                \"ts\": " + System.currentTimeMillis() + ",\n" +
                                "                \"code\": \"" + code + "\",\n" +
                                "                \"message\": \"" + error.getMessage() + "\"\n" +
                                "            }";
            } else {
                responseStr =
                                "           {\n" +
                                "                \"reqId\": \"" + UUID.randomUUID() + "\",\n" +
                                "                \"ts\": " + System.currentTimeMillis() + ",\n" +
                                "                \"data\": " + (result != null ? toJson(result) : "{}") + ",\n" +
                                "                \"code\": \"ok\"\n" +
                                "            }";
            }

            try {
                response.setContentType("application/json");
                response.getOutputStream().write(responseStr.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                response.sendError(500, e.getMessage());
            }
        });
        try {
            commandExecutionService.call(commandInfo, (result, throwable) -> {
                asyncContextManager.applyAsyncJobResult(commandInfo.getId(), result, throwable);
            });
        } catch(Throwable throwable) {
            asyncContextManager.applyAsyncJobResult(commandInfo.getId(), null, throwable);
        }
//        if(service == null || subscribeId == null) {
//            throw new BizException("Illegal arguments for subscribeId {}, subscribeId {}", service, subscribeId);
//        }

//        EventQueueService eventQueueService = InstanceFactory.instance(EventQueueService.class, "sync");
//        if(eventQueueService != null) {
//            MessageEntity message = new MessageEntity().content(content).time(new Date()).subscribeId(subscribeId).service(service);
//            eventQueueService.offer(message);
//        }
    }

    @Operation(summary = "External callback url")
    @GetMapping("id")
    public ResponseMessage<String> newId(HttpServletRequest request) {

//        if(service == null || subscribeId == null) {
//            throw new BizException("Illegal arguments for subscribeId {}, subscribeId {}", service, subscribeId);
//        }

//        EventQueueService eventQueueService = InstanceFactory.instance(EventQueueService.class, "sync");
//        if(eventQueueService != null) {
//            MessageEntity message = new MessageEntity().content(content).time(new Date()).subscribeId(subscribeId).service(service);
//            eventQueueService.offer(message);
//        }

        return success(new ObjectId().toString());
    }
}