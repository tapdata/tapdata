package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;

@Data
public class HookOneHistoryDto extends BaseDto {
    String hookId;
    String url;
    String requestBody;
    String requestParams;
    String requestHeaders;
    Long requestAt;

    String responseHeaders;
    String responseResult;
    String responseStatus;
    Integer responseCode;
    Long responseAt;

    String eventType;
    String type;

    /**
     * @see com.tapdata.tm.webhook.enums.PingResult
     */
    String status;

    /**
     * @see com.tapdata.tm.webhook.enums.WebHookHistoryStatus
     */
    String historyStatus;
    String historyMessage;
}
