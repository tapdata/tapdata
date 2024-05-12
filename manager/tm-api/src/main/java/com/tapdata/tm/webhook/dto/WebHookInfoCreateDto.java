package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.webhook.enums.PingResult;
import lombok.Data;

import java.util.List;
import java.util.Locale;

@Data
public class WebHookInfoCreateDto extends BaseDto {
    private String userId;

    /**
     * WebHook name
     */
    private String hookName;

    /**
     * WebHook URL
     */
    private String url;

    /**
     * 是否启用
     */
    private Boolean open;

    /**
     * http timout, default 5s
     */
    private Long timeout;

    /**
     * 是否默认告警方式
     */
    private Boolean defaultType;

    /**
     *
     */
    private String customTemplate;

    /**
     * ping状态
     */
    private PingResult pingResult;

    /**
     * @see com.tapdata.tm.webhook.enums.HookType hookName
     */
    private List<String> hookTypes;

    /**
     * 备注
     */
    private String mark;

    private Locale locale;
}
