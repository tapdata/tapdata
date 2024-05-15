package com.tapdata.tm.webhook.server.impl;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.webhook.dto.HookOneHistoryDto;
import com.tapdata.tm.webhook.entity.HookOneHistory;
import com.tapdata.tm.webhook.enums.ConstVariable;
import com.tapdata.tm.webhook.params.HistoryPageParam;
import com.tapdata.tm.webhook.server.WebHookHistoryService;
import com.tapdata.tm.webhook.vo.WebHookHistoryInfoVo;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;

@Service("HistoryService")
public class HistoryService implements WebHookHistoryService<WebHookHistoryInfoVo> {
    @Override
    public Page<WebHookHistoryInfoVo> list(HistoryPageParam pageParam, UserDetail userDetail, Locale locale) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public void deleteHookHistory(String hookId, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }

    @Override
    public WebHookHistoryInfoVo reSend(HookOneHistoryDto history, UserDetail user) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);    }

    @Override
    public long pushManyHistory(List<Pair<String, List<HookOneHistory>>> historyInfos) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);    }

    @Override
    public long pushHistory(String hookId, List<HookOneHistory> history) {
        throw new BizException(ConstVariable.TA_OSS_NON_SUPPORT_FUNCTION_EXCEPTION);
    }
}
