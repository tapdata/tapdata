package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.webhook.dto.WebHookInfoDto;
import com.tapdata.tm.webhook.entity.HookOneHistory;
import com.tapdata.tm.webhook.entity.WebHookEvent;
import com.tapdata.tm.webhook.server.WebHookAdapterService;

import java.util.List;

import static com.tapdata.tm.webhook.enums.ConstVariable.UN_SUPPORT_FUNCTION;

public class AdapterService implements WebHookAdapterService {
    @Override
    public void send(WebHookEvent event) {
        throw new UnsupportedOperationException(UN_SUPPORT_FUNCTION);
    }

    @Override
    public void sendAsync(WebHookEvent event, List<WebHookInfoDto> myOpenHookInfoList) {
        throw new UnsupportedOperationException(UN_SUPPORT_FUNCTION);
    }

    @Override
    public HookOneHistory sendAndSave(WebHookEvent event, WebHookInfoDto myOpenHookInfo) {
        throw new UnsupportedOperationException(UN_SUPPORT_FUNCTION);
    }

    @Override
    public HookOneHistory send(WebHookEvent event, WebHookInfoDto myOpenHookInfo) {
        throw new UnsupportedOperationException(UN_SUPPORT_FUNCTION);
    }
}
