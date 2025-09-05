package com.tapdata.tm.system.api.controller;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.system.api.dto.DebugDto;
import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.system.api.service.TextEncryptionRuleService;
import com.tapdata.tm.system.api.utils.TextEncryptionUtil;
import com.tapdata.tm.system.api.utils.ThreadPoolManager;
import com.tapdata.tm.system.api.vo.DebugVo;
import com.tapdata.tm.utils.HttpUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/8/11 17:11 Create
 * @description
 */
@Tag(name = "ApiDebug", description = "Api Debug")
@RestController
@Slf4j
@RequestMapping(value = {"/api/debug"})
public class ApiDebugController extends BaseController {
    private static final ExecutorService ASYNC_EXECUTOR = ThreadPoolManager.getAsyncTaskExecutor();

    @Resource(name = "textEncryptionRuleService")
    TextEncryptionRuleService ruleService;


    @Operation(summary = "API debug")
    @PostMapping
    public ResponseMessage<DebugVo> debug(@RequestBody DebugDto debugDto) throws ExecutionException {
        //1. Asynchronous retrieval of API sensitive field configuration and interface request results separately
        //2. Desensitize the returned data
        //3. Return result
        final CompletableFuture<Map<String, List<TextEncryptionRuleDto>>> supplyAsync = CompletableFuture.supplyAsync(
                () -> ruleService.getFieldEncryptionRuleByApiId(debugDto.getApiId()),
                ASYNC_EXECUTOR);
        final AtomicReference<Integer> httpCode = new AtomicReference<>(null);
        final CompletableFuture<DebugVo> future = CompletableFuture.supplyAsync(
                () -> http(debugDto, e -> this.updateHttpStatus(httpCode, e)),
                ASYNC_EXECUTOR);
        try {
            Map<String, List<TextEncryptionRuleDto>> objConfig = supplyAsync.get();
            DebugVo objHttp = future.get();
            if (null == objHttp) {
                objHttp = new DebugVo();
                objHttp.setHttpCode(httpCode.get());
                objHttp.setError(Map.of("message", "No data returned", "code", "NO_DATA"));
            }
            objHttp.setHttpCode(httpCode.get());
            return success(TextEncryptionUtil.map(objConfig, objHttp));
        } catch (InterruptedException e) {
            log.warn("Interrupted when get api debug result: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            return success(DebugVo.error("Interrupted when get api debug result: " + e.getMessage()));
        }
    }

    protected DebugVo http(DebugDto debugDto, HttpUtils.DoAfter after) {
        final String method = String.valueOf(debugDto.getMethod()).trim().toUpperCase();
        try {
            return switch (method) {
                case "POST" -> post(debugDto, after);
                case "GET" -> get(debugDto, after);
                default -> throw new BizException("api.debug.not.support", method);
            };
        } catch (Exception e) {
            return DebugVo.error(500, e.getMessage());
        }
    }

    DebugVo post(DebugDto debugDto, HttpUtils.DoAfter after) {
        String json = HttpUtils.sendPostData(
                debugDto.getUrl(),
                JSON.toJSONString(debugDto.getBody()),
                debugDto.getHeaders(),
                false,
                false,
                after);
        try {
            return JSON.parseObject(json, DebugVo.class);
        } catch (Exception e) {
            return DebugVo.error("Invalid data: " + json);
        }
    }

    DebugVo get(DebugDto debugDto, HttpUtils.DoAfter after) {
        String json = HttpUtils.sendGetData(
                debugDto.getUrl(),
                debugDto.getHeaders(),
                false ,
                false, after);
        try {
            return JSON.parseObject(json, DebugVo.class);
        } catch (Exception e) {
            return DebugVo.error("Invalid data: " +json);
        }
    }

    void updateHttpStatus(AtomicReference<Integer> httpCode, CloseableHttpResponse r) {
        int statusCode = r.getStatusLine().getStatusCode();
        httpCode.set(statusCode);
    }
}
