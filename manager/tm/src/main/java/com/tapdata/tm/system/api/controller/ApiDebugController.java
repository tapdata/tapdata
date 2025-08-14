package com.tapdata.tm.system.api.controller;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.system.api.dto.DebugDto;
import com.tapdata.tm.system.api.dto.TextEncryptionRuleDto;
import com.tapdata.tm.system.api.service.TextEncryptionRuleService;
import com.tapdata.tm.system.api.utils.TextEncryptionUtil;
import com.tapdata.tm.system.api.vo.DebugVo;
import com.tapdata.tm.utils.HttpUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
    @Resource(name = "textEncryptionRuleService")
    TextEncryptionRuleService ruleService;

    @Operation(summary = "API debug")
    @PostMapping
    public ResponseMessage<DebugVo> debug(@RequestBody DebugDto debugDto) throws ExecutionException {
        //1. 异步分别获取 API敏感字段配置 && 接口请求结果
        //2. 对返回数据进行脱敏
        //3. 返回结果
        final CompletableFuture<Map<String, List<TextEncryptionRuleDto>>> supplyAsync = CompletableFuture.supplyAsync(() -> ruleService.getFieldEncryptionRuleByApiId(debugDto.getApiId()));
        final CompletableFuture<DebugVo> future = CompletableFuture.supplyAsync(() -> http(debugDto));
        try {
            Map<String, List<TextEncryptionRuleDto>> objConfig = supplyAsync.get();
            DebugVo objHttp = future.get();
            return success(TextEncryptionUtil.map(objConfig, objHttp));
        } catch (InterruptedException e) {
            log.warn("Interrupted when get api debug result: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            return success(DebugVo.error("Interrupted when get api debug result: " + e.getMessage()));
        }
    }

    protected DebugVo http(DebugDto debugDto) {
        final String method = String.valueOf(debugDto.getMethod()).trim().toUpperCase();
        return switch (method) {
            case "POST" -> post(debugDto);
            case "GET" -> get(debugDto);
            default -> throw new BizException("api.debug.not.support", method);
        };
    }

    DebugVo post(DebugDto debugDto) {
        String json = HttpUtils.sendPostData(debugDto.getUrl(), JSON.toJSONString(debugDto.getBody()), debugDto.getHeaders(), false);
        try {
            return JSON.parseObject(json, DebugVo.class);
        } catch (Exception e) {
            return DebugVo.error("Invalid data: " + json);
        }
    }

    DebugVo get(DebugDto debugDto) {
        String json = HttpUtils.sendGetData(debugDto.getUrl(), debugDto.getHeaders(), false);
        try {
            return JSON.parseObject(json, DebugVo.class);
        } catch (Exception e) {
            return DebugVo.error("Invalid data: " +json);
        }
    }
}
