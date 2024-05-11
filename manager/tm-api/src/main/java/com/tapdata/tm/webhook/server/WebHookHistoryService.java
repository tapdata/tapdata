package com.tapdata.tm.webhook.server;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;

import java.util.Locale;

public interface WebHookHistoryService<V> {
    Page<V> list(String status,
                 Long start,
                 Long end,
                 String keyword,
                 Integer page,
                 Integer size,
                 UserDetail userDetail,
                 Locale locale);

    void delete(String[] ids, UserDetail userDetail);

    void deleteByHookId(String[] ids, UserDetail userDetail);

    V reSend(String historyId, UserDetail user);
}
