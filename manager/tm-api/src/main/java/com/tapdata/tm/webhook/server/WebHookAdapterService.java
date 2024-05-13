package com.tapdata.tm.webhook.server;

import com.tapdata.tm.webhook.dto.WebHookInfoDto;
import com.tapdata.tm.webhook.entity.HookOneHistory;
import com.tapdata.tm.webhook.entity.WebHookEvent;

import java.util.List;

public interface WebHookAdapterService {
    void send(WebHookEvent event);

    void sendAsync(WebHookEvent event, List<WebHookInfoDto> myOpenHookInfoList);

    HookOneHistory sendAndSave(WebHookEvent event, WebHookInfoDto myOpenHookInfo);

    HookOneHistory send(WebHookEvent event, WebHookInfoDto myOpenHookInfo);
}
