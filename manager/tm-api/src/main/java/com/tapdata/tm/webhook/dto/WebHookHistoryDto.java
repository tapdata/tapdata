package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.webhook.entity.HookOneHistory;
import lombok.Data;

import java.util.List;

@Data
public class WebHookHistoryDto extends BaseDto {
    String hookId;
    String sendBy;
    List<HookOneHistory> hookEvent;
    Boolean delete;
    long eventCount;
}
