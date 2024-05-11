package com.tapdata.tm.webhook.vo;

import com.tapdata.tm.webhook.enums.PingResult;
import lombok.Data;

import java.util.List;

@Data
public class WebHookInfoVo {
    String hookId;

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
}
