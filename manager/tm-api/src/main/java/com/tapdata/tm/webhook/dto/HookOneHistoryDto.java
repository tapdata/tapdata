package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;

@Data
public class HookOneHistoryDto extends BaseDto {
    String hookId;
    String url;
    String requestHeard;
    String requestBody;
    String requestParams;
    Long requestAt;

    String responseHeard;
    String responseResult;
    String responseStatus;
    Integer responseCode;
    Long responseAt;

    String eventType;

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
