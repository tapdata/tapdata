package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.webhook.entity.HookOneHistory;
import com.tapdata.tm.webhook.enums.ConstVariable;
import com.tapdata.tm.webhook.server.WebHookHttpUtilService;

import java.util.Map;

public class HttpUtilService implements WebHookHttpUtilService {
    @Override
    public boolean checkURL(String url) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public HookOneHistory post(String url, Map<String, Object> head, Map<String, Object> urlParam, Object body) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public HookOneHistory post(HookOneHistory history) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }
}
