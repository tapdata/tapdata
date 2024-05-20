package com.tapdata.tm.webhook.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.webhook.entity.HookOneHistory;
import lombok.Data;

import java.util.List;

@Data
public class WebHookHistoryDto extends BaseDto {
    private String hookId;
    private String sendBy;
    private List<HookOneHistory> hookEvent;
    private Boolean delete;
    private Long eventCount;
}
