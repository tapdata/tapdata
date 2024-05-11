package com.tapdata.tm.webhook.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.WebUtils;
import com.tapdata.tm.webhook.dto.WebHookInfoDto;
import com.tapdata.tm.webhook.server.WebHookService;
import com.tapdata.tm.webhook.vo.WebHookHistoryInfoVo;
import com.tapdata.tm.webhook.vo.WebHookInfoVo;
import io.swagger.v3.oas.annotations.Operation;
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


//新增webhook配置
//修改webhook配置
//删除webhook配置
//根据id查询webhook配置
//分页查询webhook配置
//查询webhook发送记录
//webhook Ping测试

/**
 * @author gavin'xiao
 * @date 2024/5/10
 */
@RestController
@RequestMapping("/api/webhook")
@Setter(onMethod_ = {@Autowired})
@Slf4j
public class WebHookController extends BaseController {
    private WebHookService<WebHookInfoVo> webHookService;

    @Operation(summary = "find all web hook info of current user")
    @GetMapping("list")
    public ResponseMessage<Page<WebHookInfoVo>> currentUserWebHookInfoList(@RequestParam(required = false) String status,
                                                                           @RequestParam(required = false) Long start,
                                                                           @RequestParam(required = false) Long end,
                                                                           @RequestParam(required = false) String keyword,
                                                                           @RequestParam(defaultValue = "1") Integer page,
                                                                           @RequestParam(defaultValue = "20") Integer size,
                                                                           HttpServletRequest request) {
        Locale locale = WebUtils.getLocale(request);
        return success(webHookService.list(status, start, end, keyword, page, size, getLoginUser(), locale));
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

    @Operation(summary = "close alarm")
    @PostMapping("close")
    public ResponseMessage<List<WebHookInfoVo>> closeWebHookByHookIds(@RequestParam String[] ids) {
        return success(webHookService.close(ids, getLoginUser()));
    }


    @Operation(summary = "close one web hook info by hook id")
    @DeleteMapping("deleteOne/{id}")
    public ResponseMessage<Void> deleteOneWebHookByHookId(@PathVariable(value = "id") String hookId) {
        webHookService.delete(new String[]{hookId}, getLoginUser());
        return success();
    }

    @Operation(summary = "close alarm")
    @DeleteMapping("delete")
    public ResponseMessage<Void> deleteWebHookByHookIds(@RequestParam String[] ids) {
        webHookService.delete(ids, getLoginUser());
        return success();
    }

    @Operation(summary = "ping test")
    @DeleteMapping("ping")
    public ResponseMessage<WebHookHistoryInfoVo> ping(@RequestBody WebHookInfoDto dto) {
        return success(webHookService.ping(dto, getLoginUser()));
    }
}
