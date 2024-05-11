package com.tapdata.tm.webhook.server;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.webhook.dto.WebHookInfoDto;
import com.tapdata.tm.webhook.vo.WebHookHistoryInfoVo;

import java.util.List;
import java.util.Locale;

public interface WebHookService<V> {
    Page<V> list(String status,
                 Long start,
                 Long end,
                 String keyword,
                 Integer page,
                 Integer size,
                 UserDetail userDetail,
                 Locale locale);

    V findWebHookByHookId(String hookId, UserDetail user);

    V update(WebHookInfoDto dto, UserDetail user);

    List<V> close(String[] ids, UserDetail userDetail);

    void delete(String[] ids, UserDetail userDetail);

    WebHookHistoryInfoVo ping(WebHookInfoDto dto, UserDetail user);
}
