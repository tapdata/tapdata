package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.webhook.dto.HookOneHistoryDto;
import com.tapdata.tm.webhook.dto.WebHookInfoDto;
import com.tapdata.tm.webhook.enums.ConstVariable;
import com.tapdata.tm.webhook.server.WebHookService;
import com.tapdata.tm.webhook.vo.WebHookInfoVo;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;

@Service("WebHookDefaultService")
public class WebHookDefaultService implements WebHookService<WebHookInfoVo> {
    @Override
    public Page<WebHookInfoVo> list(Filter filter, UserDetail userDetail, Locale locale) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public WebHookInfoVo findWebHookByHookId(String hookId, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public WebHookInfoVo create(WebHookInfoDto dto, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public WebHookInfoVo update(WebHookInfoDto dto, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public WebHookInfoVo updatePingResult(WebHookInfoDto dto) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<WebHookInfoVo> close(String[] ids, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void delete(String[] ids, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<WebHookInfoDto> findMyOpenHookInfoList(String hookType, String metric, List<String> userId) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void checkUrl(String url) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public HookOneHistoryDto ping(WebHookInfoDto webHookEvent, UserDetail userDetail) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public List<WebHookInfoVo> reOpen(String[] strings, UserDetail loginUser) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }
}
