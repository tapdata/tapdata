package com.tapdata.tm.webhook.server;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.webhook.entity.HookOneHistory;
import com.tapdata.tm.webhook.params.HistoryPageParam;
import com.tapdata.tm.webhook.vo.WebHookHistoryInfoVo;
import org.springframework.data.util.Pair;

import java.util.List;
import java.util.Locale;

public interface WebHookHistoryService<V> {

    Page<V> list(HistoryPageParam pageParam, UserDetail userDetail, Locale locale);

    WebHookHistoryInfoVo reSend(String hookId, String historyId, UserDetail user);

    long pushHistory(String hookId, List<HookOneHistory> history);

    long pushManyHistory(List<Pair<String,List<HookOneHistory>>> historyInfos);

    void deleteHookHistory(String hookId, UserDetail user);
}
