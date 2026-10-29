package com.tapdata.tm.proxyprocess.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandRequest;
import com.tapdata.tm.proxyprocess.dto.ProxyProcessCommandResult;
import com.tapdata.tm.proxyprocess.service.ProxyProcessCommandService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("/api/agent-process")
@Tag(name = "Agent Process Command", description = "代理进程自定义接口同步穿透")
public class ProxyProcessCommandController extends BaseController {
    private final ProxyProcessCommandService commandService;

    public ProxyProcessCommandController(ProxyProcessCommandService commandService) {
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
}
