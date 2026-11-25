package com.tapdata.tm.proxyprocess.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.proxyprocess.dto.AgentProcessClientSecretDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessQueryDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterResponseDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessStatusDto;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandRequest;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandResult;
import com.tapdata.tm.proxyprocess.service.AgentProcessService;
import com.tapdata.tm.proxyprocess.service.ProxyProcessCommandService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.util.List;

@RestController
@RequestMapping("/api/agent-process")
@Tag(name = "Agent Process", description = "代理进程注册、状态和管理接口")
public class AgentProcessController extends BaseController {
    private final AgentProcessService agentProcessService;
    private final ProxyProcessCommandService commandService;

    public AgentProcessController(AgentProcessService agentProcessService, ProxyProcessCommandService commandService) {
        this.agentProcessService = agentProcessService;
        this.commandService = commandService;
    }

    @PostMapping("/command")
    @Operation(summary = "同步调用代理进程自定义接口")
    public ResponseMessage<ProxyProcessCommandResult> command(@RequestBody ProxyProcessCommandRequest request) {
        return command(request.getProxyProcessId(), request.getType(), request.getValue());
    }

    ResponseMessage<ProxyProcessCommandResult> command(String proxyProcessId, String type, Object value) {
        return success(commandService.execute(proxyProcessId, type, value));
    }

    @PostMapping("/register")
    @Operation(summary = "代理进程注册")
    public ResponseMessage<AgentProcessRegisterResponseDto> register(@RequestBody AgentProcessRegisterDto request) {
        return success(agentProcessService.register(request, getLoginUser()));
    }

    @PostMapping("/status")
    @Operation(summary = "代理进程状态上报")
    public ResponseMessage<Void> reportStatus(@RequestBody AgentProcessStatusDto request) {
        agentProcessService.reportStatus(request);
        return success();
    }

    @GetMapping
    @Operation(summary = "查询代理进程列表")
    public ResponseMessage<Page<AgentProcessDto>> list(@ModelAttribute AgentProcessQueryDto query) {
        return success(agentProcessService.list(query));
    }

    @GetMapping("/types")
    @Operation(summary = "查询代理进程类型列表")
    public ResponseMessage<List<String>> listAgentTypes() {
        return success(agentProcessService.listAgentTypes());
    }

    @DeleteMapping("/{proxyProcessId}")
    @Operation(summary = "删除代理进程登记")
    public ResponseMessage<Void> delete(@PathVariable String proxyProcessId) {
        agentProcessService.delete(proxyProcessId);
        return success();
    }

    /**
     * Reveal the client secret a CDC Reader generated for itself.
     *
     * <p>Used when a connection is configured against an existing Reader: the connector needs the
     * client id, secret and token URL to obtain an access token. The secret is not part of the
     * listing, so it is fetched deliberately, one Reader at a time, by a caller that could already
     * manage that Reader.
     *
     * <p>Authentication is the standard TM login (this endpoint is deliberately not
     * {@code @IgnoreLogin}); the response is not cached.
     */
    @GetMapping("/{proxyProcessId}/client-secret")
    @Operation(summary = "读取代理进程的客户端密钥")
    public ResponseMessage<AgentProcessClientSecretDto> clientSecret(
            @PathVariable String proxyProcessId) {
        AgentProcessClientSecretDto dto = new AgentProcessClientSecretDto();
        dto.setProxyProcessId(proxyProcessId);
        String secret = agentProcessService.revealClientSecret(proxyProcessId);
        dto.setClientSecret(secret);
        return success(dto);
    }
}
