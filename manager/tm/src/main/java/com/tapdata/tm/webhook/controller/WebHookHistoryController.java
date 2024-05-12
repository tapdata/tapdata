package com.tapdata.tm.webhook.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.WebUtils;
import com.tapdata.tm.webhook.dto.HookOneHistoryDto;
import com.tapdata.tm.webhook.params.HistoryPageParam;
import com.tapdata.tm.webhook.server.WebHookHistoryService;
import com.tapdata.tm.webhook.vo.WebHookHistoryInfoVo;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;

/**
 * @author gavin'xiao
 * @date 2024/5/10
 */
@RestController
@RequestMapping("/api/webhook/history")
@Setter(onMethod_ = {@Autowired})
@Slf4j
public class WebHookHistoryController extends BaseController {
    WebHookHistoryService<WebHookHistoryInfoVo> webHookHistoryService;

    @Operation(summary = "find all web hook send history of current user")
    @GetMapping("list")
    public ResponseMessage<Page<WebHookHistoryInfoVo>> currentUserWebHookInfoList(@RequestBody HistoryPageParam pageParam,
                                                                                  HttpServletRequest request) {
        Locale locale = WebUtils.getLocale(request);
        return success(webHookHistoryService.list(pageParam, getLoginUser(), locale));
    }

    @Operation(summary = "Re-send a history message")
    @PostMapping("re-send")
    public ResponseMessage<WebHookHistoryInfoVo> reSend(@RequestBody HookOneHistoryDto historyDto) {
        return success(webHookHistoryService.reSend(historyDto, getLoginUser()));
    }
}
