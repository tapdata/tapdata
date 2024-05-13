package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.webhook.enums.PingResult;
import lombok.Data;

import java.util.List;
import java.util.Locale;

@Data
public class WebHookInfoDto extends BaseDto {
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

    private String token;

    private String httpUser;

    private String httpPwd;

    private String customHttpHead;

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
