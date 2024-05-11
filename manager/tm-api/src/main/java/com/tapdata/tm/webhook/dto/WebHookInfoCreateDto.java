package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.webhook.enums.PingResult;
import lombok.Data;

import java.util.List;
import java.util.Locale;

@Data
public class WebHookInfoCreateDto extends BaseDto {
    String userId;

    /**  WebHook name*/
    String hookName;

    /**  WebHook URL*/
    String url;

    /**  是否启用*/
    Boolean open;

    /**  http timout, default 5s*/
    Long timeout;

    /**  是否默认告警方式*/
    Boolean defaultType;

    /**  */
    String customTemplate;

    /** ping状态*/
    PingResult pingResult;

    /**
     * @see com.tapdata.tm.webhook.enums.HookType hookName
     * */
    List<String> hookTypes;

    /**备注*/
    String mark;

    Locale locale;
}
