package com.tapdata.tm.webhook.params;

import lombok.Data;

@Data
public class HistoryPageParam {
    String hookId;
    int pageFrom;
    int pageSize;
}
