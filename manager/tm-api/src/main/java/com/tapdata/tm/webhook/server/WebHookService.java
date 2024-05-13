package com.tapdata.tm.webhook.server;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.webhook.dto.WebHookInfoDto;

import java.util.List;
import java.util.Locale;

public interface WebHookService<V> {
    Page<V> list(Filter filter, UserDetail userDetail, Locale locale);

    V findWebHookByHookId(String hookId, UserDetail user);

    V create(WebHookInfoDto dto, UserDetail user);

    V update(WebHookInfoDto dto, UserDetail user);

    List<V> close(String[] ids, UserDetail userDetail);

    void delete(String[] ids, UserDetail userDetail);

    List<WebHookInfoDto> findMyOpenHookInfoList(String hookType, String metric, List<String> userId);

    void checkUrl(String url);
}
