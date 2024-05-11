package com.tapdata.tm.webhook.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.WebUtils;
import com.tapdata.tm.webhook.server.WebHookHistoryService;
import com.tapdata.tm.webhook.vo.WebHookHistoryInfoVo;
import io.swagger.v3.oas.annotations.Operation;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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

    @Operation(summary = "find all web hook info of current user")
    @GetMapping("list")
    public ResponseMessage<Page<WebHookHistoryInfoVo>> currentUserWebHookInfoList(@RequestParam(required = false) String status,
                                                                                  @RequestParam(required = false) Long start,
                                                                                  @RequestParam(required = false) Long end,
                                                                                  @RequestParam(required = false) String keyword,
                                                                                  @RequestParam(defaultValue = "1") Integer page,
                                                                                  @RequestParam(defaultValue = "20") Integer size,
                                                                                  HttpServletRequest request) {
        Locale locale = WebUtils.getLocale(request);
        return success(webHookHistoryService.list(status, start, end, keyword, page, size, getLoginUser(), locale));
    }

    @Operation(summary = "close one web hook info by hook id")
    @DeleteMapping("deleteByHookId")
    public ResponseMessage<Void> deleteOneWebHookHistoryByHistoryId(@PathVariable(value = "id") String[] hookIds) {
        webHookHistoryService.deleteByHookId(hookIds, getLoginUser());
        return success();
    }

    @Operation(summary = "close one web hook info by hook id")
    @DeleteMapping("deleteOne/{id}")
    public ResponseMessage<Void> deleteOneWebHookHistoryByHistoryId(@PathVariable(value = "id") String historyId) {
        webHookHistoryService.delete(new String[]{historyId}, getLoginUser());
        return success();
    }

    @Operation(summary = "close alarm")
    @DeleteMapping("delete")
    public ResponseMessage<Void> deleteWebHookHistoryByHistoryId(@RequestParam String[] ids) {
        webHookHistoryService.delete(ids, getLoginUser());
        return success();
    }

    @Operation(summary = "Re-send message")
    @PostMapping("re-send/{historyId}")
    public ResponseMessage<WebHookHistoryInfoVo> reSend(@PathVariable(value = "historyId") String historyId) {
        return success(webHookHistoryService.reSend(historyId, getLoginUser()));
    }
}
