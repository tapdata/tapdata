package com.tapdata.tm.webhook.server;

import com.tapdata.tm.webhook.entity.HookOneHistory;

import java.util.Map;

public interface WebHookHttpUtilService {
    boolean checkURL(String url);

    HookOneHistory post(String url,
                               Map<String, Object> head,
                               Map<String, Object> urlParam,
                               Object body);

    HookOneHistory post(HookOneHistory history);
}
