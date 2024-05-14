package com.tapdata.tm.webhook.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.WebUtils;
import com.tapdata.tm.webhook.dto.HookOneHistoryDto;
import com.tapdata.tm.webhook.dto.WebHookInfoDto;
import com.tapdata.tm.webhook.server.WebHookService;
import com.tapdata.tm.webhook.vo.WebHookInfoVo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Locale;

/**
 * @author gavin'xiao
 * @date 2024/5/10
 */
@RestController
@RequestMapping("/api/webhook")
@Setter(onMethod_ = {@Autowired})
@Slf4j
public class WebHookController extends BaseController {
    WebHookService<WebHookInfoVo> webHookService;

    @Operation(summary = "find all web hook info of current user")
    @GetMapping("list")
    public ResponseMessage<Page<WebHookInfoVo>> currentUserWebHookInfoList(@Parameter(in = ParameterIn.QUERY,
            description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
    )
                                                                           @RequestParam(value = "filter", required = false) String filterJson,
                                                                           HttpServletRequest request) {
        Locale locale = WebUtils.getLocale(request);
        return success(webHookService.list(parseFilter(filterJson), getLoginUser(), locale));
    }

    @Operation(summary = "create a web hook info")
    @PostMapping("create")
    public ResponseMessage<WebHookInfoVo> create(@RequestBody WebHookInfoDto dto, HttpServletRequest request) {
        Locale locale = WebUtils.getLocale(request);
        dto.setLocale(locale);
        return success(webHookService.create(dto, getLoginUser()));
    }

    @Operation(summary = "find web hook info by hook id")
    @GetMapping("{id}")
    public ResponseMessage<WebHookInfoVo> findWebHookByHookId(@PathVariable(value = "id") String hookId) {
        return success(webHookService.findWebHookByHookId(hookId, getLoginUser()));
    }

    @Operation(summary = "update web hook info")
    @PostMapping("update")
    public ResponseMessage<WebHookInfoVo> update(@RequestBody WebHookInfoDto dto) {
        return success(webHookService.update(dto, getLoginUser()));
    }


    @Operation(summary = "close one web hook info by hook id")
    @PostMapping("closeOne/{id}")
    public ResponseMessage<WebHookInfoVo> closeOneWebHookByHookId(@PathVariable(value = "id") String hookId) {
        List<WebHookInfoVo> closed = webHookService.close(new String[]{hookId}, getLoginUser());
        if (closed.isEmpty()) {
            return success(null);
        }
        return success(closed.get(0));
    }

    @Operation(summary = "close many web hook info by hook ids")
    @PostMapping("close")
    public ResponseMessage<List<WebHookInfoVo>> closeWebHookByHookIds(@RequestParam(value = "ids") String[] ids) {
        return success(webHookService.close(ids, getLoginUser()));
    }


    @Operation(summary = "Re-Open one web hook info by hook id")
    @PostMapping("re-open-one/{id}")
    public ResponseMessage<WebHookInfoVo> reOpenOne(@PathVariable(value = "id") String hookId) {
        List<WebHookInfoVo> closed = webHookService.reOpen(new String[]{hookId}, getLoginUser());
        if (closed.isEmpty()) {
            return failed("webhook.reOpen.failed");
        }
        return success(closed.get(0));
    }

    @Operation(summary = "Re-Open many web hook info by hook ids")
    @PostMapping("re-open")
    public ResponseMessage<List<WebHookInfoVo>> reOpenAll(@RequestParam(value = "ids") String[] ids) {
        return success(webHookService.reOpen(ids, getLoginUser()));
    }


    @Operation(summary = "delete one web hook info by hook id")
    @DeleteMapping("deleteOne/{id}")
    public ResponseMessage<Void> deleteOneWebHookByHookId(@PathVariable(value = "id") String hookId) {
        webHookService.delete(new String[]{hookId}, getLoginUser());
        return success();
    }

    @Operation(summary = "delete many web hook info by hook ids")
    @DeleteMapping("delete")
    public ResponseMessage<Void> deleteWebHookByHookIds(@RequestParam(value = "ids") String[] ids) {
        webHookService.delete(ids, getLoginUser());
        return success();
    }

    @Operation(summary = "ping test")
    @PostMapping("ping")
    public ResponseMessage<HookOneHistoryDto> ping(@RequestBody WebHookInfoDto webHookEvent) {
        ResponseMessage<HookOneHistoryDto> success = success(webHookService.ping(webHookEvent, getLoginUser()));
        success.setMessage(MessageUtil.getMessage("webhook.ping.succeed"));
        return success;
    }
}
