package com.tapdata.tm.proxyprocess.controller;

import com.tapdata.tm.base.annotation.IgnoreLogin;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.proxyprocess.dto.AgentProcessDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessQueryDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterResponseDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessStatusDto;
import com.tapdata.tm.proxyprocess.service.AgentProcessService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
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

    public AgentProcessController(AgentProcessService agentProcessService) {
        this.agentProcessService = agentProcessService;
    }

    @IgnoreLogin
    @PostMapping("/register")
    @Operation(summary = "代理进程注册")
    public ResponseMessage<AgentProcessRegisterResponseDto> register(@RequestBody AgentProcessRegisterDto request) {
        return success(agentProcessService.register(request));
    }

    @IgnoreLogin
    @PostMapping("/status")
    @Operation(summary = "代理进程状态上报")
    public ResponseMessage<Void> reportStatus(
            @RequestBody AgentProcessStatusDto request,
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        agentProcessService.reportStatus(request, authorization);
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
}
