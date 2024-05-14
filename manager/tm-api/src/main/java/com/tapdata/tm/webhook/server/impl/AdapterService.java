package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.webhook.dto.WebHookInfoDto;
import com.tapdata.tm.webhook.entity.HookOneHistory;
import com.tapdata.tm.webhook.entity.WebHookEvent;
import com.tapdata.tm.webhook.enums.ConstVariable;
import com.tapdata.tm.webhook.server.WebHookAdapterService;

import java.util.List;


public class AdapterService implements WebHookAdapterService {
    @Override
    public void send(WebHookEvent event) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void sendAsync(WebHookEvent event, List<WebHookInfoDto> myOpenHookInfoList) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public HookOneHistory sendAndSave(WebHookEvent event, WebHookInfoDto myOpenHookInfo) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public HookOneHistory send(WebHookEvent event, WebHookInfoDto myOpenHookInfo) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }
}
