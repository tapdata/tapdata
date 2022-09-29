package com.tapdata.tm.proxy.controller;

import cn.hutool.crypto.digest.MD5;
import com.tapdata.tm.async.AsyncContextManager;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.proxy.dto.*;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.modules.api.net.data.Data;
import io.tapdata.modules.api.net.data.Result;
import io.tapdata.modules.api.net.entity.SubscribeToken;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.service.CommandExecutionService;
import io.tapdata.modules.api.net.service.EventQueueService;
import io.tapdata.modules.api.net.service.node.connection.NodeConnectionFactory;
import io.tapdata.modules.api.net.service.node.connection.entity.NodeMessage;
import io.tapdata.modules.api.proxy.constants.ProxyConstants;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
//import io.tapdata.pdk.core.utils.JWTUtils;
import io.tapdata.pdk.core.utils.JWTUtils;
import io.tapdata.wsserver.channels.health.NodeHealthManager;
import io.tapdata.wsserver.channels.websocket.impl.WebSocketProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.bson.types.ObjectId;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.simplify.TapSimplify.toJson;
import static org.apache.http.HttpStatus.*;


@Tag(name = "Proxy", description = "代理网关相关接口")
@Slf4j
@RestController
@RequestMapping("/api/proxy")
public class ProxyController extends BaseController {
    private static final String TAG = ProxyController.class.getSimpleName();
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

        String nodeId = CommonUtils.getProperty("tapdata_node_id");
        if(nodeId == null)
            throw new BizException("Current nodeId not found");

        LoginProxyResponseDto loginProxyResponseDto = new LoginProxyResponseDto();
        String token = JWTUtils.createToken(key,
                map(
                        entry("nodeId", nodeId),
                        entry("service", loginProxyDto.getService().toLowerCase()),
                        entry("clientId", loginProxyDto.getClientId()),
                        entry("terminal", loginProxyDto.getTerminal()),
                        entry("uid", userDetail.getUserId()),
                        entry("cid", userDetail.getCustomerId())
                ), 30000L);
        loginProxyResponseDto.setToken(token);
        loginProxyResponseDto.setWsPort(InstanceFactory.bean(WebSocketProperties.class).getPort());
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
//        String token = JWTUtils.createToken(key,
//                map(
//                        entry("service", subscribeDto.getService().toLowerCase()),
//                        entry("subscribeId", subscribeDto.getSubscribeId())
//                ), (long)subscribeDto.getExpireSeconds() * 1000);
        SubscribeToken subscribeToken = new SubscribeToken();
        subscribeToken.setSubscribeId(subscribeDto.getSubscribeId());
        subscribeToken.setService(subscribeDto.getService());
        subscribeToken.setExpireAt(System.currentTimeMillis() + (subscribeDto.getExpireSeconds() * 1000));
        byte[] tokenBytes = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            subscribeToken.to(baos);
            tokenBytes = baos.toByteArray();
        } catch (IOException e) {
            throw new BizException("Serialize SubscribeDto failed, " + e.getMessage());
        }
        String token = null;
        try {
            token = new String(Base64.getUrlEncoder().encode(CommonUtils.encryptWithRC4(tokenBytes, key)), StandardCharsets.US_ASCII);
        } catch (Exception e) {
            throw new BizException("Encrypt SubscribeDto failed, " + e.getMessage());
        }

        SubscribeResponseDto subscribeResponseDto = new SubscribeResponseDto();
        subscribeResponseDto.setToken(token);
        return success(subscribeResponseDto);
    }
    @Operation(summary = "External callback url")
    @PostMapping("callback/{token}")
    public void rawDataCallback(@PathVariable("token") String token, @RequestBody Map<String, Object> content, HttpServletRequest request, HttpServletResponse response) throws IOException {
        if(content == null)
            throw new BizException("content is illegal, " + null);
        byte[] data = null;
        try {
            data = CommonUtils.decryptWithRC4(Base64.getUrlDecoder().decode(token.getBytes(StandardCharsets.US_ASCII)), key);
        } catch (Exception e) {
            response.sendError(SC_UNAUTHORIZED, "Token illegal");
            return;
        }
        SubscribeToken subscribeDto = new SubscribeToken();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
            subscribeDto.from(bais);
        } catch (IOException e) {
            response.sendError(SC_INTERNAL_SERVER_ERROR, "Deserialize token failed, " + e.getMessage());
            return;
        }
//        Map<String, Object> claims = JWTUtils.getClaims(key, token);
        String service = subscribeDto.getService();
        String subscribeId = subscribeDto.getSubscribeId();
        Long expireAt = subscribeDto.getExpireAt();
        if(service == null || subscribeId == null) {
            response.sendError(SC_BAD_REQUEST, FormatUtils.format("Illegal arguments for subscribeId {}, subscribeId {}", service, subscribeId));
            return;
        }
        if(expireAt != null && System.currentTimeMillis() > expireAt) {
            response.sendError(SC_UNAUTHORIZED, "Token expired");
            return;
        }

        EventQueueService eventQueueService = InstanceFactory.instance(EventQueueService.class, "sync");
        if(eventQueueService != null) {
            MessageEntity message = new MessageEntity().content(content).time(new Date()).subscribeId(subscribeId).service(service);
            eventQueueService.offer(message);
        }
        response.setStatus(SC_OK);
    }

    @Operation(summary = "External callback url")
    @PostMapping("command")
    public void command(@RequestBody CommandInfo commandInfo, HttpServletRequest request, HttpServletResponse response) {
        if(commandInfo == null)
            throw new BizException("commandInfo is illegal");

        Locale locale = WebUtils.getLocale(request);
        commandInfo.setId(UUID.randomUUID().toString().replace("-", ""));
        if(locale != null)
            commandInfo.setLocale(locale.toString());
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
        return success(new ObjectId().toString());
    }

//
//    @Operation(summary = "External callback url")
//    @PostMapping("internal")
//    public void proxyCall(HttpServletRequest request,
//                          HttpServletResponse response,
//                          @RequestParam(name = "key", required = true) String key,
//                          @RequestParam(name = "ping", required = false) String pingNodeId) {
//        try {
//            if(!key.equals(ProxyConstants.INTERNAL_KEY))
//                throw new BizException("Permission denied");
//
//            String nodeId = CommonUtils.getProperty("tapdata_node_id");
//            if(nodeId == null)
//                throw new BizException("Current nodeId not found");
//
//            if(pingNodeId != null) {
//                if(pingNodeId.equals(nodeId)) {
//                    response.setStatus(202);
//                    return;
//                } else
//                    throw new BizException("Try to visit nodeId " + pingNodeId + " but is " + nodeId);
//            }
//
//            NodeMessage nodeMessage = new NodeMessage();
//            try {
//                nodeMessage.from(request.getInputStream());
//            } catch (IOException e) {
//                throw new BizException("PostRequest resurrect failed, {}", e.getMessage());
//            }
//
//            if(!nodeId.equals(nodeMessage.getToNodeId()))
//                throw new BizException("PostRequest's nodeId {} is not current, wrong node? ", nodeMessage.getToNodeId());
//
//            NodeConnectionFactory nodeConnectionFactory = InstanceFactory.instance(NodeConnectionFactory.class);
//            if(nodeConnectionFactory == null)
//                throw new BizException("nodeConnectionFactory is not initialized");
//
//            Object responseObj = nodeConnectionFactory.received(nodeMessage.getFromNodeId(), nodeMessage.getType(), nodeMessage.getEncode(), nodeMessage.getData());
//            try (OutputStream os = response.getOutputStream()) {
//                NodeMessage responseMessage = new NodeMessage();
//                responseMessage.id(nodeMessage.getId())
//                        .fromNodeId(nodeId)
//                        .toNodeId(nodeMessage.getFromNodeId())
//                        .type(nodeMessage.getType())
//                        .time(System.currentTimeMillis())
//                        .encode(Data.ENCODE_JSON)
//                        .data(toJson(responseObj).getBytes(StandardCharsets.UTF_8));
//                responseMessage.to(os);
//            }
//            return;
//        } catch (Throwable throwable) {
//            int code = 8888;
//            CoreException coreException = null;
//            if(throwable instanceof CoreException) {
//                coreException = (CoreException) throwable;
//                code = coreException.getCode();
//            }
//            TapLogger.debug(TAG, "Internal proxy call failed, {}", ExceptionUtils.getStackTrace(throwable));
//            response.setStatus(208);
//            Result result = new Result().forId(throwable.getClass().getSimpleName()).description(throwable.getMessage()).code(code).time(System.currentTimeMillis());
//            try(OutputStream os = response.getOutputStream()) {
//                result.to(os);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//            return;
////            try {
////                response.sendError(code, throwable.getMessage());
////            } catch (Throwable ignored) {
////            }
//        }
//
//    }

    @Operation(summary = "External callback url")
    @PostMapping("internal")
    public void proxyCallNew(HttpServletRequest request,
                          HttpServletResponse response,
                          @RequestParam(name = "key", required = true) String key,
                          @RequestParam(name = "ping", required = false) String pingNodeId) {
        try {
            if(!key.equals(ProxyConstants.INTERNAL_KEY))
                throw new BizException("Permission denied");

            String nodeId = CommonUtils.getProperty("tapdata_node_id");
            if(nodeId == null)
                throw new BizException("Current nodeId not found");

            if(pingNodeId != null) {
                if(pingNodeId.equals(nodeId)) {
                    response.setStatus(202);
                    return;
                } else
                    throw new BizException("Try to visit nodeId " + pingNodeId + " but is " + nodeId);
            }

            NodeMessage nodeMessage = new NodeMessage();
            try {
                nodeMessage.from(request.getInputStream());
            } catch (IOException e) {
                throw new BizException("PostRequest resurrect failed, {}", e.getMessage());
            }
            if(nodeMessage.getId() == null)
                nodeMessage.id(UUID.randomUUID().toString().replace("-", ""));

            if(!nodeId.equals(nodeMessage.getToNodeId()))
                throw new BizException("PostRequest's nodeId {} is not current, wrong node? ", nodeMessage.getToNodeId());

            NodeConnectionFactory nodeConnectionFactory = InstanceFactory.instance(NodeConnectionFactory.class);
            if(nodeConnectionFactory == null)
                throw new BizException("nodeConnectionFactory is not initialized");

            asyncContextManager.registerAsyncJob(nodeMessage.getId(), request, (result, error) -> {
                if(error == null) {
                    try (OutputStream os = response.getOutputStream()) {
                        NodeMessage responseMessage = new NodeMessage();
                        responseMessage.id(nodeMessage.getId())
                                .fromNodeId(nodeId)
                                .toNodeId(nodeMessage.getFromNodeId())
                                .type(nodeMessage.getType())
                                .time(System.currentTimeMillis())
                                .encode(Data.ENCODE_JSON)
                                .data(toJson(result).getBytes(StandardCharsets.UTF_8));
                        responseMessage.to(os);
                        return;
                    } catch (Throwable throwable1) {
                        error = throwable1;
                    }
                }
                handleError(response, error);
            });
            try {
                nodeConnectionFactory.received(nodeMessage.getFromNodeId(), nodeMessage.getType(), nodeMessage.getEncode(), nodeMessage.getData(), (responseObj, throwable) -> {
                    asyncContextManager.applyAsyncJobResult(nodeMessage.getId(), responseObj, throwable);
                });
            } catch(Throwable throwable) {
                asyncContextManager.applyAsyncJobResult(nodeMessage.getId(), null, throwable);
            }
        } catch (Throwable throwable) {
            handleError(response, throwable);
        }
    }

    private void handleError(HttpServletResponse response, Throwable error) {
        int code = NetErrors.UNKNOWN_ERROR;
        if(error instanceof CoreException) {
            CoreException coreException = (CoreException) error;
            code = coreException.getCode();
        }
        TapLogger.debug(TAG, "Internal proxy call failed, {}", ExceptionUtils.getStackTrace(error));
        response.setStatus(208);
        Result theResult = new Result().forId(error.getClass().getSimpleName()).description(error.getMessage()).code(code).time(System.currentTimeMillis());
        try(OutputStream os = response.getOutputStream()) {
            theResult.to(os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Operation(summary = "External callback url")
    @GetMapping("cleanup")
    public ResponseMessage<List<String>> cleanUp(HttpServletRequest request) {
        NodeHealthManager nodeHealthManager = InstanceFactory.bean(NodeHealthManager.class);
        return success(nodeHealthManager.cleanUpDeadNodes());
    }

    @Operation(summary = "External callback url")
    @GetMapping("memory")
    public void memoryGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.getOutputStream().write(PDKIntegration.outputMemoryFetchers(null, "Detail").getBytes(StandardCharsets.UTF_8));
    }

    @Operation(summary = "External callback url")
    @PostMapping("memory")
    public void memory(@RequestBody MemoryDto memoryDto, HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        String keyRegex = null;
        String level = null;
        if(memoryDto != null) {
            keyRegex = memoryDto.getKeyRegex();
            String theLevel = memoryDto.getLevel();
            if(theLevel != null && (theLevel.equals("Detail") || theLevel.equals("Summary"))) {
                level = theLevel;
            }
        }
        if(level == null)
            level = "Summary";

        //exclude TapConnectorManager and PDKInvocationMonitor
        //^((?!TapConnectorManager|PDKInvocationMonitor).)*$
        response.getOutputStream().write(PDKIntegration.outputMemoryFetchers(keyRegex, level).getBytes(StandardCharsets.UTF_8));
    }
}